package impl;

// TODO: your implementation

import api.*;
import org.omg.CORBA.ORB;
import org.omg.CosNaming.NamingContextExt;
import org.omg.CosNaming.NamingContextExtHelper;
import utils.FileDesc;
import utils.FileMetadata;

import java.text.SimpleDateFormat;
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

        if (fileInfo.trim().isEmpty()) {
            return -1;
        }

        // ���ڴ��д洢�򿪵��ļ���Ϣ
        FileDesc fileDesc = FileDesc.fromString(fileInfo);
        openFiles.put((int) fileDesc.getId(), fileDesc);
        // ������ FileDesc������ NameNode�� new�����ģ�����洢ֻ��Ϊ���ÿͻ������ø���Ϣֱ���� DataNode����
        // ���ǿͻ����е� FileDesc����Ϣ�����µģ���Ϊ��д���ǿͻ���ֱ���� DataNode���������Ҫ�ӿ���ֱ�Ӹ��߿ͻ��ˣ�Ȼ����Ϣ�����ڿͻ��˲�� FileDesc

        // FIXME: �ǵ��޸ķ���ʱ�� -- �� closeʱ�ĸ����� �����һ�Σ�
        // ����û�1���ȴ򿪵�����رգ�Ӧ�ü�¼���رյ�ʱ�䣬�������Ĵ�ʱ�䣨���磩�� closeʱ�Ḳ�ǵ�����֮����ʵ��˵�ʱ��
//        fileDesc.getFileMetadata().setAccessTime(getCurrentTime());

        return (int) fileDesc.getId();
    }

    /* ��ĳ�ļ�׷��д */
    // FIXME: һ��д�������û�е� 4096��ô�죿
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
        // FIXME: ����ȫ������ DataNode��ѡ��� -- �� fileSize�жϾͺ���

        for (byte[] blockBytes : splitAndPadBytes(bytes)) {

            // ���ļ��ĵ�һ����Ҫ������
            // ��һ���ļ��ж�飬ֻ�е�һ���Ǵ��� dataBlocks �����һ������¼ append �ķ���ֵ����ʣ��Ķ��� random
            if (isOldFile && firstBytesBlockToWrite) {
                System.out.println("------ 1 ------");
                FileMetadata.DataBlock dataBlock = dataBlocks.get(dataBlocks.size() - 1);
                dataNodeID = dataBlock.getDataNodeID();
                blockID = dataBlock.getBlockID();
            } else {
                System.out.println("------ 2 ------");
                // ���ļ��ĺ��������ļ�������
                dataNodeID = getRandomDataNodeID();
                blockID = -1; // ������δ�����ģ��� block_id ��ʶΪ -1���� DataNode �ٷ���
            }

            newBlockID = dataNodes[dataNodeID].append(blockID, blockBytes);

            if (newBlockID != -1) {
                System.out.println("------ 3 ------");
                // �����ļ������ݿ���Ϣ
//                fileMetadata.addDataBlock(dataNodeID, newBlockID);
                fileDesc.getFileMetadata().addDataBlock(dataNodeID, newBlockID);
                isOldFile = true;
            } else {
                firstBytesBlockToWrite = false;
            }
        }

        // �����ļ��� size �� ����ʱ��
        fileDesc.getFileMetadata().setFileSize(fileMetadata.getFileSize() + bytes.length);
        fileDesc.getFileMetadata().setModifyTime(getCurrentTime());

        return 0;
    }

    /* ��ȡĳһ�ļ����� */
    public byte[] read(int fd) {
        FileDesc fileDesc = openFiles.get(fd);
        FileMetadata fileMetadata = fileDesc.getFileMetadata();
        List<FileMetadata.DataBlock> dataBlocks = fileMetadata.getDataBlocks();

        System.out.println("fileDesc's datablock size is " + fileDesc.getFileMetadata().getDataBlocks().size());

        // ����Ȩ��
        if (!isReadMode(fileDesc.getMode())) {
            return null;
        }

        // ����ļ��Ƿ������� -- ��ʵ����ͨ������ļ��� size��
        if (fileMetadata.getFileSize() == 0) {
            return new byte[0];  // ��û�ж�Ȩ�����ֿ�
        }

        System.out.println("fileSize = " + fileMetadata.getFileSize());

        List<byte[]> allData = new ArrayList<>();

        // ��һ���ļ������зֿ鰴��˳�򣨼�dataBlocks�еĿ�˳����֯����
        for (FileMetadata.DataBlock dataBlock : dataBlocks) {
            int dataNodeID = dataBlock.getDataNodeID();
            int blockID = dataBlock.getBlockID();

            System.out.println("dataNodeID, blockID = " + dataNodeID + " " + blockID);

            // ���Ͷ����� DataNode
            byte[] blockData = dataNodes[dataNodeID].read(blockID);

            // �����ص�������ӵ��������
            if (blockData.length > 0) {
                allData.add(extractNonZeroBytes(blockData));
            } else {
                System.out.println("Failed to read data block " + blockID + " from DataNode " + dataNodeID);
            }
        }

        System.out.println("allData list's size = " + allData.size());

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
        fileDesc.getFileMetadata().setAccessTime(getCurrentTime());

        nameNode.close(fileDesc.toString());

        // �ͷ��ڴ����ļ���Ԫ������Ϣ
        openFiles.remove(fd);

    }

    /* �ڿͻ��˶�Ҫ�洢�����ݽ����п�ͳ��Ȳ��루ÿ�����Ϊ 4096�ֽڣ� */
    private List<byte[]> splitAndPadBytes(byte[] bytes) {
        List<byte[]> blocks = new ArrayList<>();

        for (int i = 0; i < bytes.length; i += this.BLOCK_SIZE) {
            int endIndex = Math.min(i + this.BLOCK_SIZE, bytes.length);
            byte[] blockBytes = Arrays.copyOfRange(bytes, i, endIndex);

            // ��������
            if (blockBytes.length < this.BLOCK_SIZE) {
                byte[] paddedBytes = new byte[this.BLOCK_SIZE];
                System.arraycopy(blockBytes, 0, paddedBytes, 0, blockBytes.length);
                blockBytes = paddedBytes;
            }

            blocks.add(blockBytes);
        }
        return blocks;
    }

    /* ������� DataNode */
    public int getRandomDataNodeID() {
        return new Random().nextInt(MAX_DATA_NODE);  // �������Խ�����
    }

    /* ���԰����º�����ȡ�ɹ����ļ����� */

    /* ����ļ���Ȩ�� */
    private boolean isReadMode(int mode) {
        // �жϵ�λ�Ƿ�Ϊ 1
        return (mode & 0b01) != 0;
    }

    /* ����ļ�дȨ�� */
    private boolean isWriteMode(int mode) {
        // �жϸ�λ�Ƿ�Ϊ 1
        return (mode & 0b10) != 0;
    }

    /* ��ȡ��ǰʱ�� */
    public static String getCurrentTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }

    public static byte[] extractNonZeroBytes(byte[] bytes) {
        int lastIndex;
        for (lastIndex = bytes.length - 1; lastIndex >= 0; lastIndex--) {
            if (bytes[lastIndex] != 0) {
                break;
            }
        }

        if (lastIndex == -1) {
            return new byte[0];
        }
        return Arrays.copyOf(bytes, lastIndex + 1);
    }
}