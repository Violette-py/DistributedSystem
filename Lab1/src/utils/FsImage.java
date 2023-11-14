package utils;

import javax.xml.bind.annotation.*;
import java.util.List;

@XmlRootElement(name = "FsImage")
@XmlAccessorType(XmlAccessType.FIELD)
public class FsImage {
    @XmlElement(name = "fileMetadata")
    private List<FileMetadata> files;

    public List<FileMetadata> getFiles() {
        return files;
    }

    public void setFiles(List<FileMetadata> files) {
        this.files = files;
    }
}