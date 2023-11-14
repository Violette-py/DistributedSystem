package impl;

// TODO: your implementation

import api.NameNodePOA;
import utils.FileDesc;
import utils.FileMetadata;
import utils.FsImage;
import utils.FsImageXmlHandler;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.util.*;

import static utils.Constants.XML_FILE_PATH;
import static utils.UtilFunction.getCurrentTime;
import static utils.UtilFunction.isWriteMode;

public class NameNodeImpl extends NameNodePOA {

    // ��¼�򿪵��ļ��� fd��Ҳ�� FileDesc�� id������ -- ÿ�� open���󣬼�ʹ��ͬһ���ļ������ص� FileDescҲӦ�ò�ͬ��
    private static long counter = 1;
    private List<FileDesc> openFiles;

    public NameNodeImpl() {
        openFiles = new ArrayList<>();
    }

    @Override
    public String open(String filepath, int mode) {

        // ��� FsImage���Ƿ��и��ļ�
        FileMetadata fileMetadata = findFileInDisk(filepath);

        // ���ļ��������ڴ�����
        if (fileMetadata == null) {
            // �����ڴ����Ƿ��и��ļ��������ͻ��˴�������δ�־û��� FsImage�У�,ע���ȼ��Ȩ���Ƿ����
            FileDesc sameFile = null;
            for (FileDesc existingFile : openFiles) {
                if (existingFile.getFileMetadata().getFilepath().equals(filepath)) {
                    sameFile = existingFile;
                    if (isWriteMode(mode) && isWriteMode(existingFile.getMode())) {
                        // �ļ����ڱ�д�룬�����������ͻ�����дģʽ��
                        return "";
                    }
                }
            }

            // �ڴ������½��ĵ���δ���־û���¼Ԫ������Ϣ���ļ�������ֱ�Ӹ�ֵ metadata�������µĿͻ����ܶ�������д�������
            if (sameFile != null) {
                FileDesc fileDesc = new FileDesc(counter++, mode, sameFile.getFileMetadata());
                fileDesc.getFileMetadata().setAccessTime(getCurrentTime());  // ����� modifyTime ���� append���޸ĵ�
                openFiles.add(fileDesc);
                return fileDesc.toString();
            }

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

        File xmlFile = new File(XML_FILE_PATH);
        try {
            FsImageXmlHandler.marshal(fsImage, xmlFile);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }

        // ���ڴ��и� open�������Ϣɾ��
        openFiles.removeIf(value -> value.getId() == fileDesc.getId());
    }

    /* �� FsImage�в����ļ� */
    private FileMetadata findFileInDisk(String filepath) {
        FsImage fsImage = loadFsImage();

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
        File xmlFile = new File(XML_FILE_PATH);
        if (xmlFile.exists()) {
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

    /* �����ļ�Ԫ������Ϣ */
    private void updateFileMetadata(FileMetadata existingFile, FileMetadata newFile) {
        // ���������ļ�Ԫ���ݵ�������Ϣ
        existingFile.setFileSize(newFile.getFileSize());
        existingFile.setDataBlocks(newFile.getDataBlocks());
        existingFile.setModifyTime(newFile.getModifyTime());
        existingFile.setAccessTime(newFile.getAccessTime());
        // ע�⣺û���� createTime hh
    }
}