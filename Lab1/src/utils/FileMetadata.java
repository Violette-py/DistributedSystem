package utils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
//import java.util.Date;
import java.util.ArrayList;
import java.util.List;

/* XML映射类：文件的元数据信息 */
@XmlAccessorType(XmlAccessType.FIELD)
public class FileMetadata {
    @XmlElement(name = "filepath")
    private String filepath;
    @XmlElement(name = "fileSize")
    private long fileSize;    // 文件大小
    @XmlElement(name = "dataBlocks")
    private List<DataBlock> dataBlocks;  // 文件块所在位置（哪个DataNode上的哪个 block）

    // FIXME: 把时间改成 String类的，存储在 FsImage中会好看一点？
    @XmlElement(name = "createTime")
    private String createTime; // 创建时间 -- 只在 NameNode的 createNewFile函数中第一次设置
    @XmlElement(name = "modifyTime")
    private String modifyTime; // 修改时间 -- append操作完成后
    @XmlElement(name = "accessTime")
    private String accessTime; // 访问时间 -- 每一次 open请求时
    // 以上时间都是最近一次的时间

    public FileMetadata() {
    }

    // 时间应该是动态设置的，而非作为参数传入
    public FileMetadata(String filepath, long fileSize, List<DataBlock> dataBlocks) {
        this.filepath = filepath;
        this.fileSize = fileSize;
        this.dataBlocks = dataBlocks;
    }

    public FileMetadata(String filepath, long fileSize, List<DataBlock> dataBlocks, String createTime, String modifyTime, String accessTime) {
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

    public void addDataBlock(int dataNodeID, int blockID) {
        if (dataBlocks == null) {
            dataBlocks = new ArrayList<>();
        }
        DataBlock dataBlock = new DataBlock(dataNodeID, blockID);
        dataBlocks.add(dataBlock);
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(String modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getAccessTime() {
        return accessTime;
    }

    public void setAccessTime(String accessTime) {
        this.accessTime = accessTime;
    }

    // Nested class for representing data blocks
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class DataBlock {
        @XmlElement(name = "dataNodeID")
        private int dataNodeID;
        @XmlElement(name = "blockID")
        private int blockID;

        public DataBlock() {
        }

        public DataBlock(int dataNodeID, int blockID) {
            this.dataNodeID = dataNodeID;
            this.blockID = blockID;
        }


        // Getters and setters for data block fields

        public int getDataNodeID() {
            return dataNodeID;
        }

        public void setDataNodeID(int dataNodeID) {
            this.dataNodeID = dataNodeID;
        }

        public int getBlockID() {
            return blockID;
        }

        public void setBlockID(int blockID) {
            this.blockID = blockID;
        }
    }
}