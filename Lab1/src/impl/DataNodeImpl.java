package impl;

import api.DataNodePOA;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DataNodeImpl extends DataNodePOA {

    private final Map<Integer, String> blockToFileMap;
    private final String BASE_DATA_DIRECTORY = "src/resources";
    private final String dataDirectory;
    private static int dataNodeId = 1;
    private final Random random;

    public DataNodeImpl() {
        this.blockToFileMap = new HashMap<>();
        this.random = new Random();

        // 为每个 DataNode维护一个文件目录，用于存储数据文件 eg. dataNode1从 src/resources/datanode1 下读取数据
        this.dataDirectory = this.BASE_DATA_DIRECTORY + File.separator + "datanode_" + dataNodeId;
        File dir = new File(dataDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        dataNodeId++;
    }

//    public DataNodeImpl(int dataNodeId) {
//        this.dataNodeId = dataNodeId;
//        this.dataDirectory = this.BASE_DATA_DIRECTORY + File.separator + "datanode" + dataNodeId + File.separator;
//        this.blockToFileMap = new HashMap<>();
//        File dir = new File(dataDirectory);
//        if (!dir.exists()) {
//            dir.mkdirs();
//        }
//    }

    /* 读取某个数据块的所有数据 */
    @Override
    public byte[] read(int block_id) {
        if (blockToFileMap.containsKey(block_id)) {
            String filePath = blockToFileMap.get(block_id);
            try (FileInputStream fileInputStream = new FileInputStream(filePath);
                 ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
                return outputStream.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new byte[0];
    }

    /* 向某个数据块末尾追加数据 */
    // FIXME: 如果该数据块不存在应该如何处理？
    @Override
    public void append(int block_id, byte[] bytes) {
        String filePath = dataDirectory + File.separator + "block_" + block_id + ".dat";
        try (FileOutputStream fileOutputStream = new FileOutputStream(filePath, true)) {
            fileOutputStream.write(bytes);
            blockToFileMap.put(block_id, filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int randomBlockId() {
        int id = random.nextInt();
        while (blockToFileMap.containsKey(id)) {
            id = random.nextInt();
        }
        return id;
    }
}