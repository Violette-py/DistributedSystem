package impl;

import api.DataNodePOA;

import java.io.*;
import java.util.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

public class DataNodeImpl extends DataNodePOA {
    private final int BLOCK_NUM = 1024;
    private final int BLOCK_SIZE = 4 * 1024;
    private final String BASE_DATA_DIRECTORY = "src/resources";
    private final String dataDirectory;
    private static int counter = 1;
    //    private static int dataNodeID = 1;
    private int dataNodeID;
    private List<Integer> blockIDList;  // 每个 block都对应一个 .txt文件
    // 不需要在内存里记录 blockId -> byte 的映射，因为直接跟硬盘交互，只需要记录 blockid的列表即可
//    private final Map<Integer, String> blockToFileMap;

    private final Random random;

    public DataNodeImpl() {
        this.dataNodeID = counter++;
        this.blockIDList = new ArrayList<>();

        // 为每个 DataNode维护一个文件目录，用于存储数据文件 eg. dataNode1从 src/resources/datanode_1 下读取数据
        this.dataDirectory = this.BASE_DATA_DIRECTORY + File.separator + "datanode_" + this.dataNodeID;
        File dir = new File(this.dataDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        this.random = new Random();
    }

    /* 读取某个数据块的所有数据 */
    // DataNode在客户端分配，会直接调用某 DataNode的 read方法，所以这里 read只需要传 block_id即可
    @Override
    public byte[] read(int block_id) {

        String filePath = this.dataDirectory + File.separator + block_id;

        try {
            // 读取文件内容并返回
            Path path = Paths.get(filePath);
            return Files.readAllBytes(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    /* 向某个数据块末尾追加数据 */
    // FIXME: 如果该数据块不存在应该抛出异常？ -- 其实应该不会不存在，和客户端约定好了
    @Override
    public int append(int block_id, byte[] bytes) {

        // 如果 block_id 为 -1，说明是一个新文件，则随机分配一个块号
        if (block_id == -1) {
            block_id = randomBlockId();
            String filePath = this.dataDirectory + File.separator + block_id + ".txt";
            try {
                // 创建新的块文件
                Files.createFile(Paths.get(filePath));
            } catch (IOException e) {
                e.printStackTrace();
                return -1;
            }
        }

        String filePath = this.dataDirectory + File.separator + block_id + ".txt";
        File file = new File(filePath);

        // 向 block中写入数据
        try (FileOutputStream fos = new FileOutputStream(file, true)) {  // 追加写模式
            // 计算当前块剩余的空间
            long remainingSpace = this.BLOCK_SIZE - file.length();

            if (remainingSpace >= bytes.length) {
                // 如果当前块的空间足够，直接写入
                fos.write(bytes);
                return -1;
            } else {
                // 将部分数据写入当前块
                fos.write(bytes, 0, (int) remainingSpace);

                // 剩余的数据写入新块
                // 这里不用循环判断，因为客户端已经切好块了，至多分配一个新块就够了
                int newBlockId = randomBlockId();
                String newFilePath = this.dataDirectory + File.separator + "block_" + newBlockId + ".txt";
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

    @Override
    public int randomBlockId() {
        int newBlockId;
        do {
            newBlockId = new Random().nextInt(BLOCK_NUM) + 1;  // FIXME: 范围是多少
        } while (this.blockIDList.contains(newBlockId));

        this.blockIDList.add(newBlockId);
        return newBlockId;
    }
}