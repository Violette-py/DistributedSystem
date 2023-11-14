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
    private final int MAX_DATA_NODE = 2;  // Ŀǰ����ֻ�� 2�����ݽڵ�

    // ע�������� NameNode ������ NameNodeImpl
    private NameNode nameNode;
    private DataNode[] dataNodes = new DataNode[MAX_DATA_NODE];
    private Map<Integer, FileDesc> openFiles; // ��ǰ�ͻ��˴򿪵��ļ����� NameNode�������пͻ��ˣ���ͬ

    public ClientImpl() {
        // ��ʼ�� DataNode
//        dn = new DataNodeImpl[DATANODE_NUM];
//        for (int i = 0; i < DATANODE_NUM; i++) {
//            dn[i] = new DataNodeImpl();
//        }

        openFiles = new HashMap<>();

        // FIXME: ��� ids��ʲô
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

    /* �� NameNode�����ļ���Ԫ������Ϣ FileDesc */
    @Override
    public int open(String filepath, int mode) {

        // �� NameNode �����ļ���Ԫ������Ϣ��������������½������Բ��õ���Ϊ null
        String fileInfo = nameNode.open(filepath, mode);

        // ���ڴ��д洢�򿪵��ļ���Ϣ
        FileDesc fileDesc = FileDesc.fromString(fileInfo);
        openFiles.put((int) fileDesc.getId(), fileDesc);
        // ������ FileDesc������ NameNode�� new�����ģ�����洢ֻ��Ϊ���ÿͻ������ø���Ϣֱ���� DataNode����
        // ���ǿͻ����е� FileDesc����Ϣ�����µģ���Ϊ��д���ǿͻ���ֱ���� DataNode���������Ҫ�ӿ���ֱ�Ӹ��߿ͻ��ˣ�Ȼ����Ϣ�����ڿͻ��˲�� FileDesc

        return (int) fileDesc.getId();
    }

    /* ��ĳ�ļ�׷��д */
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

        // ���ļ������п�
        for (byte[] blockBytes : splitBytes(bytes)) {

            // ���ļ��ĵ�һ����Ҫ������
            // ��һ���ļ��ж�飬ֻ�е�һ���Ǵ��� dataBlocks �����һ������¼ append �ķ���ֵ����ʣ��Ķ��� random
            if (isOldFile && firstBytesBlockToWrite) {
                FileMetadata.DataBlock dataBlock = dataBlocks.get(dataBlocks.size() - 1);
                dataNodeID = dataBlock.getDataNodeID();
                blockID = dataBlock.getBlockID();
            } else {
                // ���ļ��ĺ��������ļ�������
                dataNodeID = getRandomDataNodeID();
                blockID = -1; // ������δ�����ģ��� block_id ��ʶΪ -1���� DataNode �ٷ���
            }

            newBlockID = dataNodes[dataNodeID].append(blockID, blockBytes);
            firstBytesBlockToWrite = false;
            if (newBlockID != -1) {
                // �����ļ������ݿ���Ϣ
                fileMetadata.addDataBlock(dataNodeID, newBlockID);
            }
        }

    }

    /* ��ȡĳһ�ļ����� */
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

        // ��һ���ļ������зֿ鰴��˳�򣨼�dataBlocks�еĿ�˳����֯����
        for (FileMetadata.DataBlock dataBlock : dataBlocks) {
            int dataNodeID = dataBlock.getDataNodeID();
            int blockID = dataBlock.getBlockID();

            // ���Ͷ����� DataNode
            byte[] blockData = dataNodes[dataNodeID].read(blockID);

            // �����ص�������ӵ��������
            if (blockData.length > 0) {
                allData.add(blockData);
            } else {
                System.out.println("Failed to read data block " + blockID + " from DataNode " + dataNodeID);
            }
        }

        // ���������ݿ���֯��һ��
        int totalLength = allData.stream().mapToInt(bytes -> bytes.length).sum();
        byte[] result = new byte[totalLength];
        int destPos = 0;
        for (byte[] blockData : allData) {
            System.arraycopy(blockData, 0, result, destPos, blockData.length);
            destPos += blockData.length;
        }

        return result;
    }

    /* �رմ򿪵��ļ� */
    @Override
    public void close(int fd) {

        // �� NameNode�������󣬽��ļ�Ԫ���ݸ��³־û��� FsImage��
        FileDesc fileDesc = openFiles.get(fd);
        nameNode.close(fileDesc.toString());

        // �ͷ��ڴ����ļ���Ԫ������Ϣ
        openFiles.remove(fd);

    }

    /* �ڿͻ��˶�Ҫ�洢�����ݽ����п� */
    private List<byte[]> splitBytes(byte[] bytes) {
        List<byte[]> blocks = new ArrayList<>();
        for (int i = 0; i < bytes.length; i += this.BLOCK_SIZE) {
            int end = Math.min(bytes.length, i + this.BLOCK_SIZE);
            byte[] block = Arrays.copyOfRange(bytes, i, end);
            blocks.add(block);
        }
        return blocks;
    }

    /* ������� DataNode */
    public int getRandomDataNodeID() {
        return new Random().nextInt(MAX_DATA_NODE);  // �������Խ�����
    }
}