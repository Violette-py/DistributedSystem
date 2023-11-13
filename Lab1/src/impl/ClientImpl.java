package impl;

// TODO: your implementation

import api.Client;
import utils.FileDesc;

import java.util.Map;

public class ClientImpl implements Client {

    private static NameNodeImpl nn;
    private Map<Integer, FileDesc> openFiles; // 当前客户端打开的文件，和 NameNode（存所有客户端）不同

    // 向 NameNode请求文件的元数据信息，包括 fd（fileDesc.id，fileMetadata，mode）
    @Override
    public int open(String filepath, int mode) {

        // 向 NameNode 请求文件的元数据信息，若不存在则会新建，所以不用担心为 null
        String fileInfo = nn.open(filepath, mode);

        // 在内存中存储打开的文件信息
        FileDesc fileDesc = FileDesc.fromString(fileInfo);
        openFiles.put((int) fileDesc.getId(), fileDesc); // 真正的 FileDesc都是在 NameNode中 new出来的，这里存储只是为了让客户端利用该信息直接与 DataNode交互

        return (int) fileDesc.getId();
    }

    @Override
    public void append(int fd, byte[] bytes) {

    }

    @Override
    public byte[] read(int fd) {
        return new byte[0];
    }

    @Override
    public void close(int fd) {

    }
}
