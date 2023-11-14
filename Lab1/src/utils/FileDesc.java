package utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

// TODO: According to your design, complete the FileDesc class, which wraps the information returned by NameNode open()
public class FileDesc {
    /* the id should be assigned uniquely during the lifetime of NameNode,
     * so that NameNode can know which client's open has over at close
     * e.g., on nameNode1
     * client1 opened file "Hello.txt" with mode 'w' , and retrieved a FileDesc with 0x889
     * client2 tries opening the same file "Hello.txt" with mode 'w' , and since the 0x889 is not closed yet, the return
     * value of open() is null.
     * after a while client1 call close() with the FileDesc of id 0x889.
     * client2 tries again and get a new FileDesc with a new id 0x88a
     */

    /* ������������Ѿ�˵���ˣ���ͬ���ļ��������˲�ͬ�� id����ô�Ϳ��԰������ id���� fd */
    private final long id;   // �ļ� fd ��ÿ���Ϸ��� open���󶼻�Ψһ����һ����
    private int mode;       // ��дģʽ
    private FileMetadata fileMetadata;

    public FileDesc(long id, int mode, FileMetadata fileMetadata) {
        this.id = id;
        this.mode = mode;
        this.fileMetadata = fileMetadata;
    }

    /* The following method is for conversion, so we can have interface that return string, which is easy to write in idl */

    /* �� FileDesc����Ϣ��֯���ַ������� */
    @Override
    public String toString() {
        // ע����֯���ַ����ĸ�ʽ˳�򣬺��淴����ҲҪ����ͬ���ĸ�ʽ
        StringBuilder builder = new StringBuilder();
        builder.append(id).append(",");
        builder.append(mode).append(",");
        builder.append(fileMetadata.getFilepath()).append(",");
        builder.append(fileMetadata.getFileSize()).append(",");
        builder.append(dataBlocksToString(fileMetadata.getDataBlocks())).append(",");
        builder.append(fileMetadata.getCreateTime()).append(",");
        builder.append(fileMetadata.getModifyTime()).append(",");
        builder.append(fileMetadata.getAccessTime()).append(",");
        return builder.toString();
    }

    /* ����һ����ʽ���ַ����н����� FileDesc����Ϣ */
    public static FileDesc fromString(String str) {
        if (str == null || str.trim().isEmpty()) {
            return null;
        }

        String[] parts = str.split(",");
        long id = Long.parseLong(parts[0]);
        int mode = Integer.parseInt(parts[1]);
        String filepath = parts[2];
        long fileSize = Long.parseLong(parts[3]);
        List<FileMetadata.DataBlock> dataBlocks = stringToDataBlocks(parts[4]);
        String createTime = parts[5];
        String modifyTime = parts[6];
        String accessTime = parts[7];
        FileMetadata fileMetadata = new FileMetadata(filepath, fileSize, dataBlocks, createTime, modifyTime, accessTime);
        return new FileDesc(id, mode, fileMetadata);
    }

    // ������������ List<DataBlock> ת��Ϊ�ַ���
    private static String dataBlocksToString(List<FileMetadata.DataBlock> dataBlocks) {
        if (dataBlocks == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (FileMetadata.DataBlock block : dataBlocks) {
            sb.append(block.getDataNodeID()).append(":").append(block.getBlockID()).append(";");
        }
        return sb.toString();
    }

    // �������������ַ���ת��Ϊ List<DataBlock>
    private static List<FileMetadata.DataBlock> stringToDataBlocks(String str) {
        List<FileMetadata.DataBlock> dataBlocks = new ArrayList<>();
        if (!str.equals("")) {
            String[] blockStrs = str.split(";");
            for (String blockStr : blockStrs) {
                String[] blockParts = blockStr.split(":");
                int dataNodeID = Integer.parseInt(blockParts[0]);
                int blockID = Integer.parseInt(blockParts[1]);
                dataBlocks.add(new FileMetadata.DataBlock(dataNodeID, blockID));
            }
        }
        return dataBlocks;
    }

    // Getters
    public long getId() {
        return id;
    }

    public int getMode() {
        return mode;
    }

    public FileMetadata getFileMetadata() {
        return fileMetadata;
    }
}