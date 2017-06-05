package io.openmessaging.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * @author GuoHao02@baidu.com
 * @version 2017/6/2 16:35
 */
class BucketWriter {
    private int segIdx = 0;
    private String dir;
    private String bucket;
    private int producerId;
    private FileChannel fc;
    private MappedByteBuffer buf;

    BucketWriter(String dir, String bucket, int producerId) {
        this.dir = dir;
        this.bucket = bucket;
        this.producerId = producerId;
        nextSeg();
    }

    void append(DefaultBytesMessage message) {
        if (!DefaultBytesMessage.writeTo(message, buf)) {
            nextSeg();
            DefaultBytesMessage.writeTo(message, buf);
        }
    }

    void flush() {
        try {
            fc.truncate(buf.position());
            fc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void nextSeg() {
        if (fc != null) {
            flush();
        }
        String filePrefix = dir + "/" + producerId + "/" + bucket;
        Path path = Paths.get(filePrefix);
        if (Files.notExists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        String fileName = filePrefix + "/" + segIdx++;

        path = Paths.get(fileName);
        try {
            fc = FileChannel.open(path, StandardOpenOption.WRITE,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE);
            buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 100);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
