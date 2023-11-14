package impl;

// TODO: your implementation

import api.*;
import org.omg.CORBA.ORB;
import org.omg.CosNaming.NamingContextExt;
import org.omg.CosNaming.NamingContextExtHelper;
import utils.FileDesc;
import utils.FileMetadata;

import java.util.*;

public class ClientImpl implements Client {
    private final int BLOCK_SIZE = 4 * 1024;
    private final int MAX_DATA_NODE = 2;  // 目前假设只有 2个数据节点

    // 注意这里是 NameNode 而不是 NameNodeImpl
    private NameNode nameNode;
    private DataNode[] dataNodes = new DataNode[MAX_DATA_NODE];
    private Map<Integer, FileDesc> openFiles; // 当前客户端打开的文件，和 NameNode（存所有客户端）不同

    public ClientImpl() {
        // 初始化 DataNode
//        dn = new DataNodeImpl[DATANODE_NUM];
//        for (int i = 0; i < DATANODE_NUM; i++) {
//            dn[i] = new DataNodeImpl();
//        }

        openFiles = new HashMap<>();

        // FIXME: 这个 ids是什么
//        Arrays.fill(ids, -1);
        try {
            String[] args = {};
            Properties properties = new Properties();
            properties.put("org.omg.CORBA.ORBInitialHost", "127.0.0.1"); // ORB IP
            properties.put("org.omg.CORBA.ORBInitialPort", "1050");      // 0RB port

            // new ORB object
            ORB orb = ORB.init(args, properties);

            // Naming context
            org.omg.CORBA.Object objRef = orb.resolve_initial_references("NameService");
            NamingContextExt ncRef = NamingContextExtHelper.narrow(objRef);

            // obtain a remote object
            nameNode = NameNodeHelper.narrow(ncRef.resolve_str("NameNode"));
            System.out.println("NameNode is obtained.");

            for (int dataNodeId = 0; dataNodeId < MAX_DATA_NODE; dataNodeId++) {
                dataNodes[dataNodeId] = DataNodeHelper.narrow(ncRef.resolve_str("DataNode" + dataNodeId));
                System.out.println("DataNode" + dataNodeId + "is obtained.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /* 向 NameNode请求文件的元数据信息 FileDesc */
    @Override
    public int open(String filepath, int mode) {

        // 向 NameNode 请求文件的元数据信息，若不存在则会新建，所以不用担心为 null
        String fileInfo = nameNode.open(filepath, mode);

        // 在内存中存储打开的文件信息
        FileDesc fileDesc = FileDesc.fromString(fileInfo);
        openFiles.put((int) fileDesc.getId(), fileDesc);
        // 真正的 FileDesc都是在 NameNode中 new出来的，这里存储只是为了让客户端利用该信息直接与 DataNode交互
        // 但是客户端中的 FileDesc的信息是最新的，因为读写都是客户端直接与 DataNode交互，如果要加块是直接告诉客户端，然后将信息更新在客户端侧的 FileDesc

        return (int) fileDesc.getId();
    }

    /* 向某文件追加写 */
    @Override
    public void append(int fd, byte[] bytes) {
        FileDesc fileDesc = openFiles.get(fd);
        FileMetadata fileMetadata = fileDesc.getFileMetadata();
        List<FileMetadata.DataBlock> dataBlocks = fileMetadata.getDataBlocks();

        int dataNodeID;
        int blockID;
        int newBlockID;
        boolean isOldFile = (dataBlocks != null);
        boolean firstBytesBlockToWrite = true;

        // 对文件进行切块
        for (byte[] blockBytes : splitBytes(bytes)) {

            // 老文件的第一个需要单独存
            // 若一个文件有多块，只有第一块是存在 dataBlocks 的最后一个（记录 append 的返回值），剩余的都是 random
            if (isOldFile && firstBytesBlockToWrite) {
                FileMetadata.DataBlock dataBlock = dataBlocks.get(dataBlocks.size() - 1);
                dataNodeID = dataBlock.getDataNodeID();
                blockID = dataBlock.getBlockID();
            } else {
                // 老文件的后续和新文件的所有
                dataNodeID = getRandomDataNodeID();
                blockID = -1; // 对于尚未分配块的，将 block_id 标识为 -1，到 DataNode 再分配
            }

            newBlockID = dataNodes[dataNodeID].append(blockID, blockBytes);
            firstBytesBlockToWrite = false;
            if (newBlockID != -1) {
                // 更新文件的数据块信息
                fileMetadata.addDataBlock(dataNodeID, newBlockID);
            }
        }

    }

    /* 读取某一文件数据 */
    public byte[] read(int fd) {
        FileDesc fileDesc = openFiles.get(fd);
        FileMetadata fileMetadata = fileDesc.getFileMetadata();
        List<FileMetadata.DataBlock> dataBlocks = fileMetadata.getDataBlocks();

        if (dataBlocks == null || dataBlocks.isEmpty()) {
            return null;
//            System.out.println("No data blocks available for reading.");
//            return new byte[0];
        }

        List<byte[]> allData = new ArrayList<>();

        // 把一个文件的所有分块按照顺序（即dataBlocks中的块顺序）组织起来
        for (FileMetadata.DataBlock dataBlock : dataBlocks) {
            int dataNodeID = dataBlock.getDataNodeID();
            int blockID = dataBlock.getBlockID();

            // 发送读请求到 DataNode
            byte[] blockData = dataNodes[dataNodeID].read(blockID);

            // 将返回的数据添加到结果集中
            if (blockData.length > 0) {
                allData.add(blockData);
            } else {
                System.out.println("Failed to read data block " + blockID + " from DataNode " + dataNodeID);
            }
        }

        // 将所有数据块组织在一起
        int totalLength = allData.stream().mapToInt(bytes -> bytes.length).sum();
        byte[] result = new byte[totalLength];
        int destPos = 0;
        for (byte[] blockData : allData) {
            System.arraycopy(blockData, 0, result, destPos, blockData.length);
            destPos += blockData.length;
        }

        return result;
    }

    /* 关闭打开的文件 */
    @Override
    public void close(int fd) {

        // 向 NameNode发送请求，将文件元数据更新持久化到 FsImage中
        FileDesc fileDesc = openFiles.get(fd);
        nameNode.close(fileDesc.toString());

        // 释放内存中文件的元数据信息
        openFiles.remove(fd);

    }

    /* 在客户端对要存储的数据进行切块 */
    private List<byte[]> splitBytes(byte[] bytes) {
        List<byte[]> blocks = new ArrayList<>();
        for (int i = 0; i < bytes.length; i += this.BLOCK_SIZE) {
            int end = Math.min(bytes.length, i + this.BLOCK_SIZE);
            byte[] block = Arrays.copyOfRange(bytes, i, end);
            blocks.add(block);
        }
        return blocks;
    }

    /* 随机分配 DataNode */
    public int getRandomDataNodeID() {
        return new Random().nextInt(MAX_DATA_NODE);  // 检查数组越界情况
    }
}