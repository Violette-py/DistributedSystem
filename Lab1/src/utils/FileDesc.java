package utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

    /* 上面这个例子已经说明了，相同的文件被返回了不同的 id，那么就可以把这里的 id理解成 fd */
    private final long id;
    private FileMetadata fileMetadata;

//    private long fileSize;    // 文件大小
//    private List<DataBlock> dataBlocks;  // 文件块所在位置（哪个DataNode上的哪个block）
//    private Date createTime; // 创建时间
//    private Date modifyTime; // 修改时间
//    private Date accessTime; // 访问时间
    private int mode;        // 读写模式 -- 这是和 FIleMetadata不同的地方；在内存中操作

    public FileDesc(long id, FileMetadata fileMetadata, int mode) {
        this.id = id;
        this.fileMetadata = fileMetadata;
//        this.fileSize = fileMetadata.getFileSize();
//        this.createTime = fileMetadata.getCreateTime();
//        this.modifyTime = fileMetadata.getModifyTime();
//        this.accessTime = fileMetadata.getAccessTime(); // FIXME: 是否需要更新成现在？ 还是说是 close完才算一次访问？
        this.mode = mode;

        // FIXME: 如何让两个类引用同一个公共类（xml限制） -- 目前暂时用循环赋值的方法解决
        // this.dataBlocks = fileMetadata.getDataBlocks();
//        this.dataBlocks = convertFileMetadataDataBlocks(fileMetadata.getDataBlocks());
    }

//    public FileDesc(long id, long fileSize, List<DataBlock> dataBlocks, Date createTime, Date modifyTime, Date accessTime, int mode) {
//        this.id = id;
//        this.fileSize = fileSize;
//        this.dataBlocks = dataBlocks;
//        this.createTime = createTime;
//        this.modifyTime = modifyTime;
//        this.accessTime = accessTime;
//        this.mode = mode;
//    }

    /* The following method is for conversion, so we can have interface that return string, which is easy to write in idl */

    /* 把 FileDesc的信息组织成字符串返回 */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(id).append(",");
        builder.append(fileSize).append(",");
        builder.append(dataBlocksToString(dataBlocks)).append(",");
        builder.append(dateToString(createTime)).append(",");
        builder.append(dateToString(modifyTime)).append(",");
        builder.append(dateToString(accessTime)).append(",");
        builder.append(mode);
        return builder.toString();
    }

    /* 按照一定格式从字符串中解析出 FileDesc的信息 */
    public static FileDesc fromString(String str) {
        String[] parts = str.split(",");
        long id = Long.parseLong(parts[0]);
        long fileSize = Long.parseLong(parts[1]);
        List<DataBlock> dataBlocks = stringToDataBlocks(parts[2]);
        Date createTime = stringToDate(parts[3]);
        Date modifyTime = stringToDate(parts[4]);
        Date accessTime = stringToDate(parts[5]);
        int mode = Integer.parseInt(parts[6]);
        return new FileDesc(id, fileSize, dataBlocks, createTime, modifyTime, accessTime, mode);
    }

    public long getId() {
        return id;
    }

    // 内部类
//    public static class DataBlock {
//        private int dataNodeId;
//        private int blockId;
//
//        public DataBlock(int dataNodeId, int blockId) {
//            this.dataNodeId = dataNodeId;
//            this.blockId = blockId;
//        }
//
//        // Getters and setters for data block fields
//
//        public int getDataNodeId() {
//            return dataNodeId;
//        }
//
//        public int getBlockID() {
//            return blockId;
//        }
//    }

    // 辅助方法：将 List<FileMetadata.DataBlock> 转换为 List<DataBlock>
//    private static List<DataBlock> convertFileMetadataDataBlocks(List<FileMetadata.DataBlock> fileMetadataDataBlocks) {
//        List<DataBlock> dataBlocks = new ArrayList<>();
//        for (FileMetadata.DataBlock dataBlock : fileMetadataDataBlocks) {
//            DataBlock dataBlockTemp = new DataBlock(dataBlock.getDataNodeID(), dataBlock.getBlockID());
//            dataBlocks.add(dataBlockTemp);
//        }
//        return dataBlocks;
//    }

    // 辅助方法：将 List<DataBlock> 转换为字符串
    private static String dataBlocksToString(List<FileMetadata.DataBlock> dataBlocks) {
        StringBuilder sb = new StringBuilder();
        for (FileMetadata.DataBlock block : dataBlocks) {
            sb.append(block.getDataNodeId()).append(":").append(block.getBlockId()).append(";");
        }
        return sb.toString();
    }

    // 辅助方法：将字符串转换为 List<DataBlock>
    private static List<FileMetadata.DataBlock> stringToDataBlocks(String str) {
        List<FileMetadata.DataBlock> dataBlocks = new ArrayList<>();
        String[] blockStrs = str.split(";");
        for (String blockStr : blockStrs) {
            String[] blockParts = blockStr.split(":");
            int dataNodeID = Integer.parseInt(blockParts[0]);
            int blockID = Integer.parseInt(blockParts[1]);
            dataBlocks.add(new FileMetadata.DataBlock(dataNodeID, blockID));
        }
        return dataBlocks;
    }

    // 辅助方法：将 Date 转换为字符串
    private static String dateToString(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(date);
    }

    // 辅助方法：将字符串转换为 Date
    private static Date stringToDate(String str) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return dateFormat.parse(str);
        } catch (Exception e) {
            // 处理异常
            return null;
        }
    }

}