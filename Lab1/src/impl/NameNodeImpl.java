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

    // 记录打开的文件的 fd（也即 FileDesc的 id的增长 -- 每个 open请求，即使读同一个文件，返回的 FileDesc也应该不同）
    private static long counter = 1;
    private List<FileDesc> openFiles;

    public NameNodeImpl() {
        openFiles = new ArrayList<>();
    }

    @Override
    public String open(String filepath, int mode) {

        // 检查 FsImage中是否有该文件
        FileMetadata fileMetadata = findFileInDisk(filepath);

        // 若文件不存在于磁盘中
        if (fileMetadata == null) {
            // 查找内存中是否有该文件（其他客户端创建但还未持久化到 FsImage中）,注意先检查权限是否兼容
            FileDesc sameFile = null;
            for (FileDesc existingFile : openFiles) {
                if (existingFile.getFileMetadata().getFilepath().equals(filepath)) {
                    sameFile = existingFile;
                    if (isWriteMode(mode) && isWriteMode(existingFile.getMode())) {
                        // 文件正在被写入，不允许其他客户端以写模式打开
                        return "";
                    }
                }
            }

            // 内存中有新建的但还未被持久化记录元数据信息的文件，这样直接赋值 metadata可以让新的客户端能读到最新写入的数据
            if (sameFile != null) {
                FileDesc fileDesc = new FileDesc(counter++, mode, sameFile.getFileMetadata());
                fileDesc.getFileMetadata().setAccessTime(getCurrentTime());  // 具体的 modifyTime 是在 append中修改的
                openFiles.add(fileDesc);
                return fileDesc.toString();
            }

            // 磁盘和内存中均没有对应文件，则新建
            fileMetadata = createNewFile(filepath);
        } else {
            // 文件存在磁盘中，需要检查当前的操作模式是否兼容
            for (FileDesc existingFile : openFiles) {
                if (existingFile.getFileMetadata().getFilepath().equals(filepath)) {
                    if (isWriteMode(mode) && isWriteMode(existingFile.getMode())) {
                        // 文件正在被写入，不允许其他客户端以写模式打开
                        return null;
                    }
                }
            }
        }

        // 生成新的文件描述符（FileDesc）
        FileDesc fileDesc = new FileDesc(counter++, mode, fileMetadata);
        openFiles.add(fileDesc);

        return fileDesc.toString();
    }

    /* 关闭文件，同时将文件的元数据信息（而非文件数据本身）更新持久化到硬盘中 */
    @Override
    public void close(String fileInfo) {

        // 加载当前的 FsImage
        FsImage fsImage = loadFsImage();
        List<FileMetadata> existingFiles = fsImage.getFiles();

        FileDesc fileDesc = FileDesc.fromString(fileInfo);
        FileMetadata newFileMetadata = fileDesc.getFileMetadata();

        // 更新持久化到 FsImage中：若已存在该文件（路径一致），则更新其余信息；若不存在，则新增
        boolean fileExists = false;
        for (FileMetadata existingFile : existingFiles) {
            if (existingFile.getFilepath().equals(newFileMetadata.getFilepath())) {
                updateFileMetadata(existingFile, newFileMetadata);
                fileExists = true;
                break;
            }
        }

        // 如果不存在相同路径的文件元数据，添加新的文件数据信息
        if (!fileExists) {
            existingFiles.add(newFileMetadata);
        }

        File xmlFile = new File(XML_FILE_PATH);
        try {
            FsImageXmlHandler.marshal(fsImage, xmlFile);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }

        // 把内存中该 open请求的信息删掉
        openFiles.removeIf(value -> value.getId() == fileDesc.getId());
    }

    /* 在 FsImage中查找文件 */
    private FileMetadata findFileInDisk(String filepath) {
        FsImage fsImage = loadFsImage();

        for (FileMetadata fileMetadata : fsImage.getFiles()) {
            if (fileMetadata.getFilepath().equals(filepath)) {
                return fileMetadata;
            }
        }
        return null;
    }

    /* 路径指向不存在文件时，新建文件 */
    // 这里的创建实际上是在内存中暂时创建，还未持久化到 FsImage
    private FileMetadata createNewFile(String filepath) {
        String time = getCurrentTime();
        return new FileMetadata(filepath, 0, new ArrayList<>(), time, time, time); // 新建文件时，尚未分配 block
    }

    /* 从 XML文件中加载 FsImage对象来进一步检索具体信息 */
    private FsImage loadFsImage() {
        FsImage fsImageTemp = null;
        File xmlFile = new File(XML_FILE_PATH);
        if (xmlFile.exists()) {
            try {
                fsImageTemp = FsImageXmlHandler.unmarshal(xmlFile);  // 从 xml文件中读取 FsImage对象
            } catch (JAXBException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("xmlFile not exist");
            fsImageTemp = new FsImage(); // 若 xml文件不存在  // TODO: 项目启动时，是否需要写一个 xml文件的初始化
        }
        return fsImageTemp;
    }

    /* 更新文件元数据信息 */
    private void updateFileMetadata(FileMetadata existingFile, FileMetadata newFile) {
        // 更新现有文件元数据的其他信息
        existingFile.setFileSize(newFile.getFileSize());
        existingFile.setDataBlocks(newFile.getDataBlocks());
        existingFile.setModifyTime(newFile.getModifyTime());
        existingFile.setAccessTime(newFile.getAccessTime());
        // 注意：没更新 createTime hh
    }
}