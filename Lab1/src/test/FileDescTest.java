package test;

import org.junit.Test;
import utils.FileDesc;
import utils.FileMetadata;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FileDescTest {

    @Test
    public void testToString() {
        // 创建测试数据
        List<FileMetadata.DataBlock> dataBlocks = new ArrayList<>();
        dataBlocks.add(new FileMetadata.DataBlock(1, 100));
        dataBlocks.add(new FileMetadata.DataBlock(2, 200));
        dataBlocks.add(new FileMetadata.DataBlock(3, 300));

        Calendar calendar = Calendar.getInstance();
        calendar.set(2023, Calendar.NOVEMBER, 13, 12, 0, 0); // 设置日期时间为 2023-11-13 12:00:00
        Date createTime = calendar.getTime();

        FileMetadata fileMetadata = new FileMetadata("testFile.txt", 1024, dataBlocks, createTime, createTime, createTime);
        FileDesc fileDesc = new FileDesc(123, 1, fileMetadata);

        // 调用 toString 方法并验证输出
        String expected = "123,1,testFile.txt,1024,1:100;2:200;3:300;,2023-11-13 12:00:00,2023-11-13 12:00:00,2023-11-13 12:00:00,";
        assertEquals(expected, fileDesc.toString());
    }

    @Test
    public void testFromString() {
        // 创建测试数据，手动设置假数据
        String input = "123,1,testFile.txt,1024,1:100;2:200;3:300,2023-11-13 12:00:00,2023-11-13 12:00:00,2023-11-13 12:00:00";

        // 调用 fromString 方法并验证结果
        FileDesc fileDesc = FileDesc.fromString(input);

        // 验证从字符串中解析出的对象的字段值
        assertEquals(123, fileDesc.getId());
        assertEquals(1, fileDesc.getMode());
        assertEquals("testFile.txt", fileDesc.getFileMetadata().getFilepath());
        assertEquals(1024, fileDesc.getFileMetadata().getFileSize());

        // 验证 dataBlocks 字段
        List<FileMetadata.DataBlock> dataBlocks = fileDesc.getFileMetadata().getDataBlocks();
        assertEquals(3, dataBlocks.size());
        assertEquals(1, dataBlocks.get(0).getDataNodeId());
        assertEquals(100, dataBlocks.get(0).getBlockId());
        assertEquals(2, dataBlocks.get(1).getDataNodeId());
        assertEquals(200, dataBlocks.get(1).getBlockId());
        assertEquals(3, dataBlocks.get(2).getDataNodeId());
        assertEquals(300, dataBlocks.get(2).getBlockId());

        // 验证日期字段
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        assertEquals("2023-11-13 12:00:00", dateFormat.format(fileDesc.getFileMetadata().getCreateTime()));
        assertEquals("2023-11-13 12:00:00", dateFormat.format(fileDesc.getFileMetadata().getModifyTime()));
        assertEquals("2023-11-13 12:00:00", dateFormat.format(fileDesc.getFileMetadata().getAccessTime()));
    }

}
