package test;

import api.DataNode;
import impl.DataNodeImpl;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;
import static utils.UtilFunction.extractNonZeroBytes;

public class DataNodeTest {
    static DataNodeImpl dn;

    @Before
    public void setUp() {
        dn = new DataNodeImpl(0);
//        dn = new DataNodeImpl();
    }

    @Test
    public void testRead() {
        int blockId = dn.randomBlockId();
        assertNotNull(dn.read(blockId));
    }

    @Test
    public void testAppend() {
        int blockId = dn.randomBlockId();
        byte[] toWrite = "Hello World".getBytes(StandardCharsets.UTF_8);

        dn.append(blockId, toWrite);
        byte[] read = dn.read(blockId);
        read = extractNonZeroBytes(read);  // 需要清洗数据

        int n = toWrite.length;
        int N = read.length;
        for (int i = 0; i < n; i++) {
            assertEquals("Block ID: " + blockId + ". Read block bytes and appended bytes differ at the " + i
                    + " byte to the eof.", toWrite[n - 1 - i], read[N - 1 - i]);
        }
    }
}
