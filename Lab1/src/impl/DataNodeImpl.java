package impl;

import api.DataNodePOA;

import java.io.*;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

public class DataNodeImpl extends DataNodePOA {
    private final int BLOCK_NUM = 1024;
    private final int BLOCK_SIZE = 4 * 1024;
    private final String BASE_DATA_DIRECTORY = "src/resources";
    private final String dataDirectory;
    private static int counter = 0;
    //    private static int dataNodeID = 1;
    private int dataNodeID;
    private List<Integer> blockIDList;  // ÿ�� block����Ӧһ�� .txt�ļ�
    // ����Ҫ���ڴ����¼ blockId -> byte ��ӳ�䣬��Ϊֱ�Ӹ�Ӳ�̽�����ֻ��Ҫ��¼ blockid���б���
//    private final Map<Integer, String> blockToFileMap;

    private final Random random;

    public DataNodeImpl() {
        this.dataNodeID = counter++;
        this.blockIDList = new ArrayList<>();

        // Ϊÿ�� DataNodeά��һ���ļ�Ŀ¼�����ڴ洢�����ļ� eg. dataNode1�� src/resources/datanode_1 �¶�ȡ����
        this.dataDirectory = this.BASE_DATA_DIRECTORY + File.separator + "datanode_" + this.dataNodeID;
        File dir = new File(this.dataDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        this.random = new Random();
    }

    /* ��ȡĳ�����ݿ���������� */
    // DataNode�ڿͻ��˷��䣬��ֱ�ӵ���ĳ DataNode�� read�������������� readֻ��Ҫ�� block_id����
    @Override
    public byte[] read(int block_id) {

        String filePath = this.dataDirectory + File.separator + "block_" + block_id + ".txt";

        try {
            // ��ȡ�ļ����ݲ�����
            Path path = Paths.get(filePath);
            return Files.readAllBytes(path);
        } catch (NoSuchFileException e) {
            return new byte[0];
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    /* ��ĳ�����ݿ�ĩβ׷������ */
    // FIXME: ��������ݿ鲻����Ӧ���׳��쳣�� -- ��ʵӦ�ò��᲻���ڣ��Ϳͻ���Լ������
    @Override
    public int append(int block_id, byte[] bytes) {

        // ��� block_id Ϊ -1��˵����һ�����ļ������������һ�����
        if (block_id <= 0) {
            block_id = randomBlockId();
            createNewBlockFile(block_id);
        } else if (blockIDList.isEmpty() || blockIDList.contains(block_id)) {
            // �����Ӧ�� block�������ڣ���Ҫ�����������Դ��룩
            createNewBlockFile(block_id);
        }

        String filePath = this.dataDirectory + File.separator + "block_" + block_id + ".txt";

        File file = new File(filePath);

        // �� block��д������
        try (FileOutputStream fos = new FileOutputStream(file, true)) {  // ׷��дģʽ
            // ���㵱ǰ��ʣ��Ŀռ�
            long remainingSpace = this.BLOCK_SIZE - file.length();

            if (remainingSpace >= bytes.length) {
                // �����ǰ��Ŀռ��㹻��ֱ��д��
                fos.write(bytes);
                return -1;
            } else {
                // ����������д�뵱ǰ��
                fos.write(bytes, 0, (int) remainingSpace);

                // ʣ�������д���¿�
                // ���ﲻ��ѭ���жϣ���Ϊ�ͻ����Ѿ��кÿ��ˣ��������һ���¿�͹���
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

    @Override
    public int randomBlockId() {
        int newBlockId;
        do {
            newBlockId = new Random().nextInt(BLOCK_NUM) + 1;  // FIXME: ��Χ�Ƕ���
        } while (this.blockIDList.contains(newBlockId));

        this.blockIDList.add(newBlockId);
        return newBlockId;
    }

    /* �����µĿ��ļ� block_id.txt */
    private boolean createNewBlockFile(int block_id) {
        String filePath = this.dataDirectory + File.separator + "block_" + block_id + ".txt";

        try {
            // �����µĿ��ļ�
            Files.createFile(Paths.get(filePath));
            return true;  // ���ش����Ŀ��ID
        } catch (IOException e) {
            e.printStackTrace();
            return false;  // ����-1��ʾ����ʧ��
        }
    }

}