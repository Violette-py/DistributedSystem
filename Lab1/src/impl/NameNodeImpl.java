package impl;

// TODO: your implementation

import api.NameNodePOA;
import utils.FileDesc;
import utils.FileMetadata;
import utils.FsImage;
import utils.FsImageXmlHandler;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class NameNodeImpl extends NameNodePOA {
    private final String FILE_BASE_DIRECTORY = "";

    /* NameNode维护一个持久化的 FsImage，其中记录了文件目录结构和所有文件的元数据信息 */
    FsImage fsImage;

    // 记录打开的文件的 fd（也即 FileDesc的 id的增长 -- 每个 open请求，即使读同一个文件，返回的 FileDesc也应该不同）
    private static long counter = 1;

    // 保存 fd - FileDesc 的键值对，其中 fd 就是 FileDesc的 id，但是为了便于查找，所以提出来写成 Map的形式
    private Map<Long, FileDesc> openFiles;

//    private final Map<String, FileDesc> openFiles;
    // FIXME: 这样记录有个问题，如果对同一个文件有多个读请求，应该返回不同的 FileDesc(id不同)，这样写会导致返回同一个
    // FIXME: 感觉不需要记录所有打开的文件，只需要记录正在被写的文件用于判断 open请求的合法性即可
    // FIXME: 但是也不能只记录被写的文件，应该记录所有 open请求对应的 FileDesc，这样才可以在客户端执行 append、read、close操作（参数都是fd）

    public NameNodeImpl() {
        openFiles = new HashMap<>();
        fsImage = loadFsImage();
    }

//    @Override
//    public synchronized String open(String filepath, int mode) {
//    public String open(String filepath, int mode) {

    // 检查当前路径文件是否存在（查询 FsImage），不存在则新建

//        if (openFiles.containsKey(filepath)) {
//            FileDesc existingFile = openFiles.get(filepath);
//            // 同一个文件只能有一个写的 open 请求，但可以有多个读的 open 请求
//            if ((existingFile.getId() & mode) == 0) {
//                // 当前文件正在被其他客户端读
//                return existingFile.toString();
//            } else {
//                // 当前文件正在被其他客户端写
//                return null;
//            }
//        } else {
//            long fileId = System.currentTimeMillis(); // 每个文件对应一个独立的 id -- 通过时间戳确保唯一性 // FIXME： 是否会有多线程的问题
//            FileDesc newFile = new FileDesc(fileId);
//            openFiles.put(filepath, newFile);
//            return newFile.toString();
//        }
//    }

    @Override
    public String open(String filepath, int mode) {
        // 检查当前路径文件是否存在（查询 FsImage）
        FileMetadata fileMetadata = findFile(filepath);

        if (fileMetadata != null) {
            // 文件存在，返回文件的元数据信息
            long fileId = counter++;
            FileDesc fileDesc = new FileDesc(fileId, mode, fileMetadata);
            openFiles.put(fileId, fileDesc);
            return fileDesc.toString();
        } else {
            // 文件不存在，新建文件
            FileMetadata newFileMetadata = createNewFile(filepath);
            long fileId = counter++;
            FileDesc fileDesc = new FileDesc(fileId, mode, newFileMetadata);
            openFiles.put(fileId, fileDesc);
            return fileDesc.toString();
        }
    }

    /* 关闭文件，同时将文件的元数据信息（而非文件数据本身）更新持久化到硬盘中 */
    @Override
    public void close(String fileInfo) {
        FileDesc fileDesc = FileDesc.fromString(fileInfo);
        openFiles.values().removeIf(value -> value.getId() == fileDesc.getId());
    }

    // 根据文件路径查找文件
    private FileMetadata findFile(String filepath) {
        for (FileMetadata file : fsImage.getFiles()) {
            if (file.getFilepath().equals(filepath)) {
                return file;
            }
        }
        return null; // 文件不存在
    }

    // 创建新文件
    private FileMetadata createNewFile(String filepath) {

        // FIXME: 这里的创建到底是指在哪里创建？

        FileMetadata newFile = new FileMetadata(filepath, 0, new ArrayList<>());
//        newFile.setFilepath(filepath);
        // 设置其他元数据信息，如大小、创建时间等
        return newFile;
    }

    private FsImage loadFsImage() {
        FsImage fsImageTemp = null;
        File xmlFile = new File("src/resources/FsImage.xml"); // TODO: 提取成常量
        if (xmlFile.exists()) {
            try {
                fsImageTemp = FsImageXmlHandler.unmarshal(xmlFile);  // 从 xml文件中读取 FsImage对象
            } catch (JAXBException e) {
                e.printStackTrace();
            }
        } else {
            fsImageTemp = new FsImage(); // 若 xml文件不存在  // TODO: 项目启动时，是否需要写一个 xml文件的初始化
        }
        return fsImageTemp;
    }
}