package utils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import java.util.Date;
import java.util.List;

/* XML映射类：文件的元数据信息 */
@XmlAccessorType(XmlAccessType.FIELD)
public class FileMetadata {
    private String filepath;
    private long fileSize;    // 文件大小
    private List<DataBlock> dataBlocks;  // 文件块所在位置（哪个DataNode上的哪个block）
    private Date createTime; // 创建时间
    private Date modifyTime; // 修改时间
    private Date accessTime; // 访问时间   // FIXME: 是否需要更新成现在？ 还是说是 close完才算一次访问？

    // FIXME: 需要再写一个构造函数，首次创建文件时，设置创建时间（而不是作为参数传入）
    public FileMetadata(String filepath, long fileSize, List<DataBlock> dataBlocks) {
        this.filepath = filepath;
        this.fileSize = fileSize;
        this.dataBlocks = dataBlocks;
    }

    public FileMetadata(String filepath, long fileSize, List<DataBlock> dataBlocks, Date createTime, Date modifyTime, Date accessTime) {
        this.filepath = filepath;
        this.fileSize = fileSize;
        this.dataBlocks = dataBlocks;
        this.createTime = createTime;
        this.modifyTime = modifyTime;
        this.accessTime = accessTime;
    }

    // Getters and setters for all fields

    public String getFilepath() {
        return filepath;
    }

    public void setFilepath(String filepath) {
        this.filepath = filepath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public List<DataBlock> getDataBlocks() {
        return dataBlocks;
    }

    public void setDataBlocks(List<DataBlock> dataBlocks) {
        this.dataBlocks = dataBlocks;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }

    public Date getAccessTime() {
        return accessTime;
    }

    public void setAccessTime(Date accessTime) {
        this.accessTime = accessTime;
    }

    // Nested class for representing data blocks
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class DataBlock {
        private int dataNodeId;
        private int blockId;

        public DataBlock(int dataNodeId, int blockId) {
            this.dataNodeId = dataNodeId;
            this.blockId = blockId;
        }


        // Getters and setters for data block fields

        public int getDataNodeId() {
            return dataNodeId;
        }

        public void setDataNodeId(int dataNodeId) {
            this.dataNodeId = dataNodeId;
        }

        public int getBlockId() {
            return blockId;
        }

        public void setBlockId(int blockId) {
            this.blockId = blockId;
        }
    }
}