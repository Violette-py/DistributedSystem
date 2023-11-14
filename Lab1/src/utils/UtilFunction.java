package utils;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class UtilFunction {

    /* ����ļ���Ȩ�� */
    public static boolean isReadMode(int mode) {
        // �жϵ�λ�Ƿ�Ϊ 1
        return (mode & 0b01) != 0;
    }

    /* ����ļ�дȨ�� */
    public static boolean isWriteMode(int mode) {
        // �жϸ�λ�Ƿ�Ϊ 1
        return (mode & 0b10) != 0;
    }

    /* ��ȡ��ǰʱ�� */
    public static String getCurrentTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }

    /* �������ݣ���ȡ����Ķ��������� */
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
