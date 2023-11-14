package impl;

// TODO: your implementation

import api.*;
import org.omg.CORBA.ORB;
import org.omg.CosNaming.NamingContextExt;
import org.omg.CosNaming.NamingContextExtHelper;
import utils.FileDesc;
import utils.FileMetadata;

import java.util.*;

import static utils.Constants.BLOCK_SIZE;
import static utils.Constants.MAX_DATA_NODE;
import static utils.UtilFunction.*;

public class ClientImpl implements Client {
    private NameNode nameNode; // 注意这里是 NameNode 而不是 NameNodeImpl
    private final DataNode[] dataNodes = new DataNode[MAX_DATA_NODE];
    private Map<Integer, FileDesc> openFiles; // 当前客户端打开的文件，和 NameNode（存所有客户端）不同
    // 真正的 FileDesc都是在 NameNode中 new出来的，这里存储只是为了让客户端利用该信息直接与 DataNode交互
    // 但是客户端中的 FileDesc的信息是最新的，因为读写都是客户端直接与 DataNode交互，如果要加块是直接告诉客户端，然后将信息更新在客户端侧的 FileDesc

    public ClientImpl() {
        openFiles = new HashMap<>();

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
                System.out.println("DataNode " + dataNodeId + " is obtained.");
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

        // 返回空串，说明权限不兼容（不能同时写同一个文件）
        if (fileInfo.trim().isEmpty()) {
            return -1;
        }

        // 在内存中存储打开的文件信息
        FileDesc fileDesc = FileDesc.fromString(fileInfo);
        openFiles.put((int) fileDesc.getId(), fileDesc);

        // 在 close时再修改访问时间

        return (int) fileDesc.getId();
    }

    /* 向某文件追加写 */
    @Override
    public int append(int fd, byte[] bytes) {
        FileDesc fileDesc = openFiles.get(fd);
        FileMetadata fileMetadata = fileDesc.getFileMetadata();
        List<FileMetadata.DataBlock> dataBlocks = fileMetadata.getDataBlocks();

        // 检查文件是否有写权限
        if (!isWriteMode(fileDesc.getMode())) {
            return -1;
        }

        int dataNodeID;
        int blockID;
        int newBlockID;
        boolean isOldFile = dataBlocks.size() > 0;
        boolean firstBytesBlockToWrite = true;

        // 对文件进行切块
        for (byte[] blockBytes : splitAndPadBytes(bytes)) {

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

            // 调用 DataNode追加数据
            newBlockID = dataNodes[dataNodeID].append(blockID, blockBytes);
            if (newBlockID != -1) {
                // 更新文件的数据块信息
                fileDesc.getFileMetadata().addDataBlock(dataNodeID, newBlockID);
                isOldFile = true;
            } else {
                firstBytesBlockToWrite = false;
            }
        }

        // 更新文件的 size 和 修改时间
        fileDesc.getFileMetadata().setFileSize(fileMetadata.getFileSize() + bytes.length);
        fileDesc.getFileMetadata().setModifyTime(getCurrentTime());

        return 0;
    }

    /* 读取某一文件数据 */
    public byte[] read(int fd) {
        FileDesc fileDesc = openFiles.get(fd);
        FileMetadata fileMetadata = fileDesc.getFileMetadata();
        List<FileMetadata.DataBlock> dataBlocks = fileMetadata.getDataBlocks();

        // 检查读权限
        if (!isReadMode(fileDesc.getMode())) {
            return null;
        }

        // 检查文件是否有数据
        if (fileMetadata.getFileSize() == 0) {
            return new byte[0];  // 和没有读权限区分开
        }

        List<byte[]> allData = new ArrayList<>();

        // 把一个文件的所有分块按照顺序（即 dataBlocks中的块顺序）组织起来
        for (FileMetadata.DataBlock dataBlock : dataBlocks) {
            int dataNodeID = dataBlock.getDataNodeID();
            int blockID = dataBlock.getBlockID();

            // 发送读请求到 DataNode
            byte[] blockData = dataNodes[dataNodeID].read(blockID);

            // 将返回的数据添加到结果集中
            if (blockData.length > 0) {
                allData.add(extractNonZeroBytes(blockData));
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
        FileDesc fileDesc = openFiles.get(fd);

        // 更新文件访问时间
        fileDesc.getFileMetadata().setAccessTime(getCurrentTime());
        // 向 NameNode发送请求，将文件元数据更新持久化到 FsImage中
        nameNode.close(fileDesc.toString());
        // 释放内存中文件的元数据信息
        openFiles.remove(fd);
    }

    /* 在客户端对要存储的数据进行切块和长度补齐（每块必须为 4096字节） */
    private List<byte[]> splitAndPadBytes(byte[] bytes) {
        List<byte[]> blocks = new ArrayList<>();

        for (int i = 0; i < bytes.length; i += BLOCK_SIZE) {
            int endIndex = Math.min(i + BLOCK_SIZE, bytes.length);
            byte[] blockBytes = Arrays.copyOfRange(bytes, i, endIndex);

            // 补齐数据
            if (blockBytes.length < BLOCK_SIZE) {
                byte[] paddedBytes = new byte[BLOCK_SIZE];
                System.arraycopy(blockBytes, 0, paddedBytes, 0, blockBytes.length);
                blockBytes = paddedBytes;
            }

            blocks.add(blockBytes);
        }
        return blocks;
    }

    /* 随机分配 DataNode */
    public int getRandomDataNodeID() {
        return new Random().nextInt(MAX_DATA_NODE);
    }

}