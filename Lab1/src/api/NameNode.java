package api;
//TODO: This file serves as a reference/inspiration, your should generate your DataNode interface using idjl command
//  to be more specific, your generated file NameNodePOA should look like this file.

public interface NameNode {
    /**
     *
     * @param mode (0b01-R, 0b10-W, 0b11-R/W)
     * if file does not exist, create.
     * @return FileDesc for the specified file if Mode is permitted, else null
     * e.g., if the file is opened with W mode, but other client has the same file opened previously with mode W or WR,
     * then this call should return null.
     */
    String open(String filepath, int mode);

    void close(String fileInfo);

}
