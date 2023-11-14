package utils;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.File;

public class FsImageXmlHandler {

    /* 将 FsImage对象写入xml文件 */
    public static void marshal(FsImage fsImage, File xmlFile) throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(FsImage.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(fsImage, xmlFile);
    }

    /* 从 xml文件中读取 FsImage对象 */
    public static FsImage unmarshal(File xmlFile) throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(FsImage.class);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        return (FsImage) unmarshaller.unmarshal(xmlFile);
    }
}