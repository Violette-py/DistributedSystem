package impl;
//TODO: your implementation
import api.Client;
public class ClientImpl implements Client{

    @Override
    public int open(String filepath, int mode) {
        return 0;
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
