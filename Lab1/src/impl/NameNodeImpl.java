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

    /* NameNode维护一个持久化的 FsImage，其中记录了文件目录结构和所有文件的元数据信息 */
    // FsImage fsImage;

    // 记录打开的文件的 fd（也即 FileDesc的 id的增长 -- 每个 open请求，即使读同一个文件，返回的 FileDesc也应该不同）
    private static long counter = 1;

    // 保存 fd - FileDesc 的键值对，其中 fd 就是 FileDesc的 id，但是为了便于查找，所以提出来写成 Map的形式
    private List<FileDesc> openFiles;
//    private Map<Long, FileDesc> openFiles; // 客户端记录这种映射关系可以便于对应 fd， NameNode端就不需要了

    public NameNodeImpl() {
        openFiles = new ArrayList<>();
//        openFiles = new HashMap<>();
        // fsImage = loadFsImage();
        // 当然不是在 NameNode启动时就把 FsImage load进来，因为后面 FsImage还会改 -> 应该在用的地方再 load
    }

    @Override
    public String open(String filepath, int mode) {

        System.out.println("---- NameNode / open ----");

        // 注意：openFiles的实时更新 -- 每次 new FileDesc都要加入 List

        // 首先检查 FsImage中是否有该文件
        FileMetadata fileMetadata = findFileInDisk(filepath);
        System.out.println("after find file in disk");

        // 若文件不存在于磁盘中
        if (fileMetadata == null) {

            System.out.println("file is not in disk");

            // 查找内存中是否有该文件（其他客户端创建但还未持久化到 FsImage中）
            for (FileDesc existingFile : openFiles) {
                if (existingFile.getFileMetadata().getFilepath().equals(filepath)) {
                    System.out.println("file is created but still not stored in FsImage");
                    if (isWriteMode(mode) && isWriteMode(existingFile.getMode())) {
                        // 文件正在被写入，不允许其他客户端以写模式打开
                        return null;
                    } else {
                        long fileId = counter++;
                        // 内存中有新建的但还未被持久化记录元数据信息的文件，这样直接赋值 metadata可以让新的客户端能读到最新写入的数据
                        FileDesc fileDesc = new FileDesc(fileId, mode, existingFile.getFileMetadata());
                        fileDesc.getFileMetadata().setAccessTime(getCurrentTime());  // 具体的 modifyTime 是在 append中修改的
                        openFiles.add(fileDesc);
                        return existingFile.toString();
                    }
                }
            }
            System.out.println("file is not in memory");
            System.out.println("so now create it ...");
            // 磁盘和内存中均没有对应文件，则新建
            fileMetadata = createNewFile(filepath);
        }

        // 生成新的文件描述符（FileDesc）
        FileDesc fileDesc = new FileDesc(counter++, mode, fileMetadata);
        openFiles.add(fileDesc);

        System.out.println("new created fileDesc : " + fileDesc);

        // 返回文件描述符的字符串表示形式
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

        File xmlFile = new File("src/resources/FsImage.xml"); // TODO: 提取成常量
        try {
            FsImageXmlHandler.marshal(fsImage, xmlFile);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }

        // FIXME: 在手动测试的时候检查一下这里，单元测试因为速度太快而没有更新？
//        System.out.println("before delete FileDesc from memory, openFiles.size() : " + openFiles.size());
//        for (FileDesc existingFile: openFiles) {
//            System.out.println(existingFile.getFileMetadata().getFilepath());
//        }

        // 把内存中该 open请求的信息删掉
        openFiles.removeIf(value -> value.getId() == fileDesc.getId());

//        System.out.println("After delete FileDesc from memory, openFiles.size() : " + openFiles.size());
//        for (FileDesc existingFile: openFiles) {
//            System.out.println(existingFile.getFileMetadata().getFilepath());
//        }

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

    /* 在内存中查找文件（可能客户端新建后尚未 close，也就没有持久化到 FsImage中） */
    private FileDesc findFileInMemory(String filepath) {
        for (FileDesc fileDesc : openFiles) {
            if (fileDesc.getFileMetadata().getFilepath().equals(filepath)) {
                return fileDesc;
            }
        }
        return null;
    }

    /* 在 FsImage中查找文件 */
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

    /* 路径指向不存在文件时，新建文件 */
    // 这里的创建实际上是在内存中暂时创建，还未持久化到 FsImage
    private FileMetadata createNewFile(String filepath) {
        String time = getCurrentTime();
        return new FileMetadata(filepath, 0, new ArrayList<>(), time, time, time); // 新建文件时，尚未分配 block
    }

    /* 从 XML文件中加载 FsImage对象来进一步检索具体信息 */
    private FsImage loadFsImage() {
        FsImage fsImageTemp = null;
        File xmlFile = new File("src/resources/FsImage.xml"); // TODO: 提取成常量
        if (xmlFile.exists()) {
            System.out.println("xmlFile exist");
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

    /* 判断是否是写模式 */
    private boolean isWriteMode(int mode) {
        // 判断高位是否为 1
        return (mode & 0b10) != 0;
    }

    /* 获取当前时间 */
    public static String getCurrentTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }
}