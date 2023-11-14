package utils;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class UtilFunction {

    /* 检查文件读权限 */
    public static boolean isReadMode(int mode) {
        // 判断低位是否为 1
        return (mode & 0b01) != 0;
    }

    /* 检查文件写权限 */
    public static boolean isWriteMode(int mode) {
        // 判断高位是否为 1
        return (mode & 0b10) != 0;
    }

    /* 获取当前时间 */
    public static String getCurrentTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }

    /* 清晰数据，提取非零的二进制数据 */
    public static byte[] extractNonZeroBytes(byte[] bytes) {
        int lastIndex;
        for (lastIndex = bytes.length - 1; lastIndex >= 0; lastIndex--) {
            if (bytes[lastIndex] != 0) {
                break;
            }
        }

        if (lastIndex == -1) {
            return new byte[0];
        }
        return Arrays.copyOf(bytes, lastIndex + 1);
    }
}
