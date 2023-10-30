package test;
import utils.FileDesc;
import api.NameNode;
import impl.NameNodeImpl;
import org.junit.Before;
import org.junit.Test;
import utils.FileSystem;

import static org.junit.Assert.*;

public class NameNodeTest {
    private static NameNodeImpl nn;
    private void close(FileDesc... fileInfos){
        for(FileDesc fileInfo: fileInfos){
            nn.close(fileInfo.toString());
        }
    }

    @Before
    public void setUp(){
        nn = new NameNodeImpl();
    }

    @Test
    /* open a non-exist file */
    public void testCreate(){
        String filename = FileSystem.newFilename();
        FileDesc fileInfo = FileDesc.fromString(nn.open(filename, 0b10));
        assertNotNull(fileInfo);
        close(fileInfo);
    }

    @Test
    /* open an existing file */
    public void testOpen(){
        String filename = FileSystem.newFilename();
        FileDesc fileInfo = FileDesc.fromString(nn.open(filename, 0b10));
        FileDesc fileInfo2 = FileDesc.fromString(nn.open(filename, 0b01));
        assertNotSame(fileInfo,fileInfo2);
        close(fileInfo, fileInfo2);
    }



    @Test
    /* open an existing and being written file in writing mode */
    public void testOpenWrite(){
        String filename = FileSystem.newFilename();
        FileDesc fileInfo = FileDesc.fromString(nn.open(filename, 0b10));
        FileDesc fileInfo2 = FileDesc.fromString(nn.open(filename, 0b11));
        assertNotNull(fileInfo);
        assertNull(fileInfo2);
        close(fileInfo);
    }

    @Test
    /* open an existing and being written file in reading mode, multiple times */
    public void testOpenRead(){
        String filename = FileSystem.newFilename();
        FileDesc fileInfo = FileDesc.fromString(nn.open(filename, 0b10));
        FileDesc fileInfo2 = FileDesc.fromString(nn.open(filename, 0b01));
        FileDesc fileInfo3 = FileDesc.fromString(nn.open(filename, 0b01));
        assertNotNull(fileInfo);
        assertNotNull(fileInfo2);
        assertNotNull(fileInfo3);
        close(fileInfo,fileInfo2,fileInfo3);
    }
}
