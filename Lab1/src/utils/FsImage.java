package utils;

import javax.xml.bind.annotation.*;
import java.util.List;

@XmlRootElement(name = "FsImage")
@XmlAccessorType(XmlAccessType.FIELD)
public class FsImage {

    // TODO: ����Ҫά��һ���ļ�Ŀ¼�ṹ�����ڿ��ٲ��ҿͻ���������ļ��Ƿ���ڣ����������½��� -- �Ƿ���Ҫ��
//    @XmlElementWrapper(name = "files") // ʹ�� @XmlElementWrapper ��װ�б�
    @XmlElement(name = "fileMetadata")
    private List<FileMetadata> files;

    public List<FileMetadata> getFiles() {
        return files;
    }

    public void setFiles(List<FileMetadata> files) {
        this.files = files;
    }
}