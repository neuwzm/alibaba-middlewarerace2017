package io.openmessaging.demo;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import io.openmessaging.Message;
import sun.nio.ch.DirectBuffer;

/**
 * @author GuoHao02@baidu.com
 * @version 2017/6/2 16:53
 */
class BucketReader {
    private int curProducer = -1;
    private List<Path> segments;
    private FileChannel fc;
    private MappedByteBuffer buf;

    BucketReader(String dir, String bucket) {
        segments = SegmentStore.INST.getSegment(dir, bucket);
        nextProducer();
    }

    Message poll() {
            try {
                return DefaultBytesMessage.fromByte(buf);
            }catch (Exception e){
                if (!nextProducer()) {
                    return null;
                }
                return DefaultBytesMessage.fromByte(buf);
            }
    }

    private boolean nextProducer() {
        curProducer++;
        if (fc != null) {
            try {
                ((DirectBuffer) buf).cleaner().clean();
                fc.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (curProducer >= segments.size()) {
            return false;
        }
        Path path = segments.get(curProducer);
        try {
            fc = FileChannel.open(path, StandardOpenOption.READ);
            buf = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
