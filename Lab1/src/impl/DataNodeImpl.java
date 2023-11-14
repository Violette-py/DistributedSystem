package impl;

import api.DataNodePOA;

import java.io.*;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

import static utils.Constants.*;
import static utils.UtilFunction.extractNonZeroBytes;

public class DataNodeImpl extends DataNodePOA {
    private final String dataDirectory;
    private int dataNodeID;
    private List<Integer> blockIDList;  // 每个 block都对应一个 .txt文件

    public DataNodeImpl(int dataNodeID) {
        this.dataNodeID = dataNodeID;
        this.blockIDList = new ArrayList<>();

        // 为每个 DataNode维护一个文件目录，用于存储数据文件 eg. dataNode1从 src/resources/datanode_1 下读取数据
        this.dataDirectory = BASE_DATA_DIRECTORY + File.separator + "datanode_" + this.dataNodeID;
        File dir = new File(this.dataDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    /* 读取某个数据块的所有数据 */
    // DataNode在客户端分配，会直接调用某 DataNode的 read方法，所以这里 read只需要传 block_id即可
    @Override
    public byte[] read(int block_id) {

        String filePath = this.dataDirectory + File.separator + "block_" + block_id + ".txt";

        try {
            // 读取文件内容并返回
            Path path = Paths.get(filePath);
            byte[] blockBytes = Files.readAllBytes(path);

            // 补齐数据（ IDL接口定义要求一定为4096字节）
            if (blockBytes.length < BLOCK_SIZE) {
                byte[] paddedBytes = new byte[BLOCK_SIZE];
                System.arraycopy(blockBytes, 0, paddedBytes, 0, blockBytes.length);
                blockBytes = paddedBytes;
            }
            return blockBytes;
        } catch (NoSuchFileException e) {
            return new byte[0];
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    /* 向某个数据块末尾追加数据 */
    @Override
    public int append(int block_id, byte[] bytes) {

        // 清洗数据
        bytes = extractNonZeroBytes(bytes);

        boolean isNewBlock = false;

        // 如果 block_id 为 -1，说明是一个新文件，则随机分配一个块号
        if (block_id <= 0) {
            block_id = randomBlockId();
            createNewBlockFile(block_id);
            isNewBlock = true;
        } else if (blockIDList.isEmpty() || !blockIDList.contains(block_id)) {
            // 如果对应的 block还不存在，则要创建（看测试代码）
            createNewBlockFile(block_id);
            isNewBlock = true;
        }

        String filePath = this.dataDirectory + File.separator + "block_" + block_id + ".txt";
        File file = new File(filePath);

        // 向 block中写入数据
        try (FileOutputStream fos = new FileOutputStream(file, true)) {  // 追加写模式
            // 计算当前块剩余的空间
            long remainingSpace = BLOCK_SIZE - file.length();

            if (remainingSpace >= bytes.length) {
                // 如果当前块的空间足够，直接写入
                fos.write(bytes);
                if (isNewBlock) {
                    return block_id;
                } else {
                    return -1;
                }
            } else {
                // 将部分数据写入当前块
                fos.write(bytes, 0, (int) remainingSpace);

                // 剩余的数据写入新块
                // 这里不用循环判断，因为客户端已经切好块了，至多分配一个新块就够了
                int newBlockId = randomBlockId();
                String newFilePath = this.dataDirectory + File.separator + "block_" + block_id + ".txt";
                Files.createFile(Paths.get(newFilePath));

                try (FileOutputStream newFos = new FileOutputStream(newFilePath, true)) {
                    newFos.write(bytes, (int) remainingSpace, bytes.length - (int) remainingSpace);
                } catch (IOException e) {
                    e.printStackTrace();
                    return -1;
                }

                return newBlockId;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    /* 获取随机的 blockId */
    @Override
    public int randomBlockId() {
        int newBlockId;
        do {
            newBlockId = new Random().nextInt(BLOCK_NUM) + 1;
        } while (this.blockIDList.contains(newBlockId));

        this.blockIDList.add(newBlockId);
        return newBlockId;
    }

    /* 创建新的块文件 block_id.txt */
    private void createNewBlockFile(int block_id) {
        String filePath = this.dataDirectory + File.separator + "block_" + block_id + ".txt";

        try {
            // 创建新的块文件
            Files.createFile(Paths.get(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}