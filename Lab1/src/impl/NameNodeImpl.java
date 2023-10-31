package impl;

// TODO: your implementation

import api.NameNodePOA;
import utils.FileDesc;

import java.util.HashMap;
import java.util.Map;

public class NameNodeImpl extends NameNodePOA {

    private final Map<String, FileDesc> openFiles;

    public NameNodeImpl() {
        openFiles = new HashMap<>();
    }

    @Override
//    public synchronized String open(String filepath, int mode) {
    public String open(String filepath, int mode) {
        if (openFiles.containsKey(filepath)) {
            FileDesc existingFile = openFiles.get(filepath);
            if ((existingFile.getId() & mode) == 0) {
                // 当前文件被其他客户端打开，但是操作模式是可兼容的
                return existingFile.toString();
            } else {
                // 当前文件正在被其他客户端写
                return null;
            }
        } else {
            long fileId = System.currentTimeMillis(); // 每个文件对应一个独立的 id -- 通过时间戳确保唯一性 // FIXME： 是否会有多线程的问题
            FileDesc newFile = new FileDesc(fileId);
            openFiles.put(filepath, newFile);
            return newFile.toString();
        }
    }

    @Override
    public void close(String fileInfo) {
        FileDesc fileDesc = FileDesc.fromString(fileInfo);
        openFiles.values().removeIf(value -> value.getId() == fileDesc.getId());
    }
}