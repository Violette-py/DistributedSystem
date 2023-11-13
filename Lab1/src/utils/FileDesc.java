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
    private long mode;        // 读写模式 -- 这是和 FIleMetadata不同的地方；在内存中操作

    public FileDesc(long id, long mode, FileMetadata fileMetadata) {
        this.id = id;
        this.mode = mode;
        this.fileMetadata = fileMetadata;
    }

    /* The following method is for conversion, so we can have interface that return string, which is easy to write in idl */

    /* 把 FileDesc的信息组织成字符串返回 */
    @Override
    public String toString() {
        // 注意组织成字符串的格式顺序，后面反解析也要按照同样的格式
        StringBuilder builder = new StringBuilder();
        builder.append(id).append(",");
        builder.append(mode).append(",");
        builder.append(fileMetadata.getFilepath()).append(",");
        builder.append(fileMetadata.getFileSize()).append(",");
        builder.append(dataBlocksToString(fileMetadata.getDataBlocks())).append(",");
        builder.append(dateToString(fileMetadata.getCreateTime())).append(",");
        builder.append(dateToString(fileMetadata.getModifyTime())).append(",");
        builder.append(dateToString(fileMetadata.getAccessTime())).append(",");
        return builder.toString();
    }

    /* 按照一定格式从字符串中解析出 FileDesc的信息 */
    public static FileDesc fromString(String str) {
        String[] parts = str.split(",");
        long id = Long.parseLong(parts[0]);
        long mode = Integer.parseInt(parts[1]);
        String filepath = parts[2];
        long fileSize = Long.parseLong(parts[3]);
        List<FileMetadata.DataBlock> dataBlocks = stringToDataBlocks(parts[4]);
        Date createTime = stringToDate(parts[5]);
        Date modifyTime = stringToDate(parts[6]);
        Date accessTime = stringToDate(parts[7]);
        FileMetadata fileMetadata = new FileMetadata(filepath, fileSize, dataBlocks, createTime, modifyTime, accessTime);
        return new FileDesc(id, mode, fileMetadata);
    }

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

    // Getters
    public long getId() {
        return id;
    }

    public long getMode() {
        return mode;
    }

    public FileMetadata getFileMetadata() {
        return fileMetadata;
    }
}