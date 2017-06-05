package io.openmessaging.demo;

import java.nio.ByteBuffer;

class Util {

    static byte[] intToByte(int x) {
        byte[] data = new byte[5];
        int cnt = 0;
        do {
            data[cnt] = (byte) ((x & 0x7f) | 0x80);
            cnt++;
            x >>= 7;
        } while (x != 0);
        data[0] &= 0x7f;
        byte[] res = new byte[cnt];
        for (int i = cnt - 1, j = 0; i >= 0; i--, j++) {
            res[j] = data[i];
        }
        return res;
    }

    static int byteToInt(ByteBuffer mbb) {
        int res = 0;
        while (true) {
            byte b = mbb.get();
            res = (res << 7) | (b & 0x7f);
            if ((b & 0x80) == 0) {
                break;
            }
        }
        return res;
    }
}
