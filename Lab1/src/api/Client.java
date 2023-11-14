package api;
public interface Client {
    int open(String filepath, int mode);
    int append(int fd, byte[] bytes);

    /**
     * @param fd
     * @return All bytes in file, or null if the read is not allowed (file opened without r).
     */
    byte[] read(int fd);
    void close(int fd);
}
