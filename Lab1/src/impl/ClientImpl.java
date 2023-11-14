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
    private NameNode nameNode; // ע�������� NameNode ������ NameNodeImpl
    private final DataNode[] dataNodes = new DataNode[MAX_DATA_NODE];
    private Map<Integer, FileDesc> openFiles; // ��ǰ�ͻ��˴򿪵��ļ����� NameNode�������пͻ��ˣ���ͬ
    // ������ FileDesc������ NameNode�� new�����ģ�����洢ֻ��Ϊ���ÿͻ������ø���Ϣֱ���� DataNode����
    // ���ǿͻ����е� FileDesc����Ϣ�����µģ���Ϊ��д���ǿͻ���ֱ���� DataNode���������Ҫ�ӿ���ֱ�Ӹ��߿ͻ��ˣ�Ȼ����Ϣ�����ڿͻ��˲�� FileDesc

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

    /* �� NameNode�����ļ���Ԫ������Ϣ FileDesc */
    @Override
    public int open(String filepath, int mode) {

        // �� NameNode �����ļ���Ԫ������Ϣ��������������½������Բ��õ���Ϊ null
        String fileInfo = nameNode.open(filepath, mode);

        // ���ؿմ���˵��Ȩ�޲����ݣ�����ͬʱдͬһ���ļ���
        if (fileInfo.trim().isEmpty()) {
            return -1;
        }

        // ���ڴ��д洢�򿪵��ļ���Ϣ
        FileDesc fileDesc = FileDesc.fromString(fileInfo);
        openFiles.put((int) fileDesc.getId(), fileDesc);

        // �� closeʱ���޸ķ���ʱ��

        return (int) fileDesc.getId();
    }

    /* ��ĳ�ļ�׷��д */
    @Override
    public int append(int fd, byte[] bytes) {
        FileDesc fileDesc = openFiles.get(fd);
        FileMetadata fileMetadata = fileDesc.getFileMetadata();
        List<FileMetadata.DataBlock> dataBlocks = fileMetadata.getDataBlocks();

        // ����ļ��Ƿ���дȨ��
        if (!isWriteMode(fileDesc.getMode())) {
            return -1;
        }

        int dataNodeID;
        int blockID;
        int newBlockID;
        boolean isOldFile = dataBlocks.size() > 0;
        boolean firstBytesBlockToWrite = true;

        // ���ļ������п�
        for (byte[] blockBytes : splitAndPadBytes(bytes)) {

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

            // ���� DataNode׷������
            newBlockID = dataNodes[dataNodeID].append(blockID, blockBytes);
            if (newBlockID != -1) {
                // �����ļ������ݿ���Ϣ
                fileDesc.getFileMetadata().addDataBlock(dataNodeID, newBlockID);
                isOldFile = true;
            } else {
                firstBytesBlockToWrite = false;
            }
        }

        // �����ļ��� size �� �޸�ʱ��
        fileDesc.getFileMetadata().setFileSize(fileMetadata.getFileSize() + bytes.length);
        fileDesc.getFileMetadata().setModifyTime(getCurrentTime());

        return 0;
    }

    /* ��ȡĳһ�ļ����� */
    public byte[] read(int fd) {
        FileDesc fileDesc = openFiles.get(fd);
        FileMetadata fileMetadata = fileDesc.getFileMetadata();
        List<FileMetadata.DataBlock> dataBlocks = fileMetadata.getDataBlocks();

        // ����Ȩ��
        if (!isReadMode(fileDesc.getMode())) {
            return null;
        }

        // ����ļ��Ƿ�������
        if (fileMetadata.getFileSize() == 0) {
            return new byte[0];  // ��û�ж�Ȩ�����ֿ�
        }

        List<byte[]> allData = new ArrayList<>();

        // ��һ���ļ������зֿ鰴��˳�򣨼� dataBlocks�еĿ�˳����֯����
        for (FileMetadata.DataBlock dataBlock : dataBlocks) {
            int dataNodeID = dataBlock.getDataNodeID();
            int blockID = dataBlock.getBlockID();

            // ���Ͷ����� DataNode
            byte[] blockData = dataNodes[dataNodeID].read(blockID);

            // �����ص�������ӵ��������
            if (blockData.length > 0) {
                allData.add(extractNonZeroBytes(blockData));
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
        FileDesc fileDesc = openFiles.get(fd);

        // �����ļ�����ʱ��
        fileDesc.getFileMetadata().setAccessTime(getCurrentTime());
        // �� NameNode�������󣬽��ļ�Ԫ���ݸ��³־û��� FsImage��
        nameNode.close(fileDesc.toString());
        // �ͷ��ڴ����ļ���Ԫ������Ϣ
        openFiles.remove(fd);
    }

    /* �ڿͻ��˶�Ҫ�洢�����ݽ����п�ͳ��Ȳ��루ÿ�����Ϊ 4096�ֽڣ� */
    private List<byte[]> splitAndPadBytes(byte[] bytes) {
        List<byte[]> blocks = new ArrayList<>();

        for (int i = 0; i < bytes.length; i += BLOCK_SIZE) {
            int endIndex = Math.min(i + BLOCK_SIZE, bytes.length);
            byte[] blockBytes = Arrays.copyOfRange(bytes, i, endIndex);

            // ��������
            if (blockBytes.length < BLOCK_SIZE) {
                byte[] paddedBytes = new byte[BLOCK_SIZE];
                System.arraycopy(blockBytes, 0, paddedBytes, 0, blockBytes.length);
                blockBytes = paddedBytes;
            }

            blocks.add(blockBytes);
        }
        return blocks;
    }

    /* ������� DataNode */
    public int getRandomDataNodeID() {
        return new Random().nextInt(MAX_DATA_NODE);
    }

}