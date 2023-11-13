package utils;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
public class FsImage {

    // TODO: 还需要维护一个文件目录结构，便于快速查找客户端请求的文件是否存在（不存在则新建） -- 是否需要？
    private List<FileMetadata> files;

    @XmlElement(name = "file")
    public List<FileMetadata> getFiles() {
        return files;
    }

    public void setFiles(List<FileMetadata> files) {
        this.files = files;
    }
}
