package impl;

// TODO: your implementation

import api.NameNodePOA;
import utils.FileDesc;
import utils.FileMetadata;
import utils.FsImage;
import utils.FsImageXmlHandler;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

public class NameNodeImpl extends NameNodePOA {

    /* NameNodeά��һ���־û��� FsImage�����м�¼���ļ�Ŀ¼�ṹ�������ļ���Ԫ������Ϣ */
    // FsImage fsImage;

    // ��¼�򿪵��ļ��� fd��Ҳ�� FileDesc�� id������ -- ÿ�� open���󣬼�ʹ��ͬһ���ļ������ص� FileDescҲӦ�ò�ͬ��
    private static long counter = 1;

    // ���� fd - FileDesc �ļ�ֵ�ԣ����� fd ���� FileDesc�� id������Ϊ�˱��ڲ��ң����������д�� Map����ʽ
    private List<FileDesc> openFiles;
//    private Map<Long, FileDesc> openFiles; // �ͻ��˼�¼����ӳ���ϵ���Ա��ڶ�Ӧ fd�� NameNode�˾Ͳ���Ҫ��

    public NameNodeImpl() {
        openFiles = new ArrayList<>();
//        openFiles = new HashMap<>();
        // fsImage = loadFsImage();
        // ��Ȼ������ NameNode����ʱ�Ͱ� FsImage load��������Ϊ���� FsImage����� -> Ӧ�����õĵط��� load
    }

    @Override
    public String open(String filepath, int mode) {

        System.out.println("---- NameNode / open ----");

        // ע�⣺openFiles��ʵʱ���� -- ÿ�� new FileDesc��Ҫ���� List

        // ���ȼ�� FsImage���Ƿ��и��ļ�
        FileMetadata fileMetadata = findFileInDisk(filepath);
        System.out.println("after find file in disk");

        // ���ļ��������ڴ�����
        if (fileMetadata == null) {

            System.out.println("file is not in disk");

            // �����ڴ����Ƿ��и��ļ��������ͻ��˴�������δ�־û��� FsImage�У�,���ȼ��Ȩ���Ƿ����
            FileDesc sameFile = null;
            for (FileDesc existingFile : openFiles) {
                if (existingFile.getFileMetadata().getFilepath().equals(filepath)) {
                    sameFile = existingFile;
                    if (isWriteMode(mode) && isWriteMode(existingFile.getMode())) {
                        // �ļ����ڱ�д�룬�����������ͻ�����дģʽ��
                        return "";
//                        return null;
                    }
                }
            }

            if (sameFile != null) {

                System.out.println("file is created but still not stored in FsImage");

                // �ڴ������½��ĵ���δ���־û���¼Ԫ������Ϣ���ļ�������ֱ�Ӹ�ֵ metadata�������µĿͻ����ܶ�������д�������
                FileDesc fileDesc = new FileDesc(counter++, mode, sameFile.getFileMetadata());
                fileDesc.getFileMetadata().setAccessTime(getCurrentTime());  // ����� modifyTime ���� append���޸ĵ�
                openFiles.add(fileDesc);
                return fileDesc.toString();

            }

            System.out.println("file is not in memory");
            System.out.println("so now create it ...");
            // ���̺��ڴ��о�û�ж�Ӧ�ļ������½�
            fileMetadata = createNewFile(filepath);
        } else {
            // �ļ����ڴ����У���Ҫ��鵱ǰ�Ĳ���ģʽ�Ƿ����
            for (FileDesc existingFile : openFiles) {
                if (existingFile.getFileMetadata().getFilepath().equals(filepath)) {
                    if (isWriteMode(mode) && isWriteMode(existingFile.getMode())) {
                        // �ļ����ڱ�д�룬�����������ͻ�����дģʽ��
                        return null;
                    }
                }
            }
        }

        // �����µ��ļ���������FileDesc��
        FileDesc fileDesc = new FileDesc(counter++, mode, fileMetadata);
        openFiles.add(fileDesc);

        System.out.println("new created fileDesc : " + fileDesc);

        // �����ļ����������ַ�����ʾ��ʽ
        return fileDesc.toString();
    }

    /* �ر��ļ���ͬʱ���ļ���Ԫ������Ϣ�������ļ����ݱ������³־û���Ӳ���� */
    @Override
    public void close(String fileInfo) {

        // ���ص�ǰ�� FsImage
        FsImage fsImage = loadFsImage();
        List<FileMetadata> existingFiles = fsImage.getFiles();

        FileDesc fileDesc = FileDesc.fromString(fileInfo);
        FileMetadata newFileMetadata = fileDesc.getFileMetadata();

        // ���³־û��� FsImage�У����Ѵ��ڸ��ļ���·��һ�£��������������Ϣ���������ڣ�������
        boolean fileExists = false;
        for (FileMetadata existingFile : existingFiles) {
            if (existingFile.getFilepath().equals(newFileMetadata.getFilepath())) {
                updateFileMetadata(existingFile, newFileMetadata);
                fileExists = true;
                break;
            }
        }

        // �����������ͬ·�����ļ�Ԫ���ݣ�����µ��ļ�������Ϣ
        if (!fileExists) {
            existingFiles.add(newFileMetadata);
        }

        File xmlFile = new File("src/resources/FsImage.xml"); // TODO: ��ȡ�ɳ���
        try {
            FsImageXmlHandler.marshal(fsImage, xmlFile);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }

        // FIXME: ���ֶ����Ե�ʱ����һ�������Ԫ������Ϊ�ٶ�̫���û�и��£�
//        System.out.println("before delete FileDesc from memory, openFiles.size() : " + openFiles.size());
//        for (FileDesc existingFile: openFiles) {
//            System.out.println(existingFile.getFileMetadata().getFilepath());
//        }

        // ���ڴ��и� open�������Ϣɾ��
        openFiles.removeIf(value -> value.getId() == fileDesc.getId());

    }

    /* �����ļ�Ԫ������Ϣ */
    private void updateFileMetadata(FileMetadata existingFile, FileMetadata newFile) {
        // ���������ļ�Ԫ���ݵ�������Ϣ
        existingFile.setFileSize(newFile.getFileSize());
        existingFile.setDataBlocks(newFile.getDataBlocks());
        existingFile.setModifyTime(newFile.getModifyTime());
        existingFile.setAccessTime(newFile.getAccessTime());
        // ע�⣺û���� createTime hh
    }

    /* ���ڴ��в����ļ������ܿͻ����½�����δ close��Ҳ��û�г־û��� FsImage�У� */
    private FileDesc findFileInMemory(String filepath) {
        for (FileDesc fileDesc : openFiles) {
            if (fileDesc.getFileMetadata().getFilepath().equals(filepath)) {
                return fileDesc;
            }
        }
        return null;
    }

    /* �� FsImage�в����ļ� */
    private FileMetadata findFileInDisk(String filepath) {
//        System.out.println("before loadFsImage");
        FsImage fsImage = loadFsImage();
//        System.out.println("after loadFsImage");
//        System.out.println("fsImage.file is : " + fsImage.getFiles());
//        for (FileMetadata fileMetadata : fsImage.getFiles()) {
//            System.out.println("file path is : " + fileMetadata.getFilepath());
//        }

        for (FileMetadata fileMetadata : fsImage.getFiles()) {
            if (fileMetadata.getFilepath().equals(filepath)) {
                return fileMetadata;
            }
        }
        return null;
    }

    /* ·��ָ�򲻴����ļ�ʱ���½��ļ� */
    // ����Ĵ���ʵ���������ڴ�����ʱ��������δ�־û��� FsImage
    private FileMetadata createNewFile(String filepath) {
        String time = getCurrentTime();
        return new FileMetadata(filepath, 0, new ArrayList<>(), time, time, time); // �½��ļ�ʱ����δ���� block
    }

    /* �� XML�ļ��м��� FsImage��������һ������������Ϣ */
    private FsImage loadFsImage() {
        FsImage fsImageTemp = null;
        File xmlFile = new File("src/resources/FsImage.xml"); // TODO: ��ȡ�ɳ���
        if (xmlFile.exists()) {
            System.out.println("xmlFile exist");
            try {
                fsImageTemp = FsImageXmlHandler.unmarshal(xmlFile);  // �� xml�ļ��ж�ȡ FsImage����
            } catch (JAXBException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("xmlFile not exist");
            fsImageTemp = new FsImage(); // �� xml�ļ�������  // TODO: ��Ŀ����ʱ���Ƿ���Ҫдһ�� xml�ļ��ĳ�ʼ��
        }
        return fsImageTemp;
    }

    /* �ж��Ƿ���дģʽ */
    private boolean isWriteMode(int mode) {
        // �жϸ�λ�Ƿ�Ϊ 1
        return (mode & 0b10) != 0;
    }

    /* ��ȡ��ǰʱ�� */
    public static String getCurrentTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }
}