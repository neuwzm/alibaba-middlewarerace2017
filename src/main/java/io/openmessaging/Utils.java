package io.openmessaging;

/**
 * @author GuoHao02@baidu.com
 * @version 2017/5/27 12:34
 */
public class Utils {
    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }
}
