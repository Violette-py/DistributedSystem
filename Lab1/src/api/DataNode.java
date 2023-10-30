package api;
//TODO: This file serves as a reference/inspiration, your should generate your DataNode interface using idjl command
//  to be more specific, your generated file DataNodePOA should look like this file.

public interface DataNode {
    byte[] read(int block_id);
    void append(int block_id, byte[] bytes);

    /**
     * @return Any of the "valid" data block's id, this is purely for sake of testing.
     * The definition of valid should be included in your documentation
     */
    int randomBlockId();
}
