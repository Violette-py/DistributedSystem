package utils;

import javax.xml.bind.annotation.*;
import java.util.List;

@XmlRootElement(name = "FsImage")
@XmlAccessorType(XmlAccessType.FIELD)
public class FsImage {

    // TODO: 还需要维护一个文件目录结构，便于快速查找客户端请求的文件是否存在（不存在则新建） -- 是否需要？
//    @XmlElementWrapper(name = "files") // 使用 @XmlElementWrapper 包装列表
    @XmlElement(name = "fileMetadata")
    private List<FileMetadata> files;

    public List<FileMetadata> getFiles() {
        return files;
    }

    public void setFiles(List<FileMetadata> files) {
        this.files = files;
    }
}