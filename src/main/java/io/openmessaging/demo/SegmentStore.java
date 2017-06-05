package io.openmessaging.demo;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author GuoHao02@baidu.com
 * @version 2017/6/4 18:12
 */
class SegmentStore {
    static final SegmentStore INST = new SegmentStore();
    private HashMap<String, List<Path>> segments = new HashMap<>(255);
    private volatile int init = 0;
    private AtomicInteger lock = new AtomicInteger(0);

    private SegmentStore() {
    }

    List<Path> getSegment(String dir, String bucket) {
        if (init == 0) {
            scan(dir);
        }
        return segments.computeIfAbsent(bucket, b -> loadProducers(dir, bucket));
    }

    private void scan(String dir) {
        while (!lock.compareAndSet(0, 1)) {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (init == 0) {
            doScan(dir);
            init = 1;
        }
        lock.set(0);
    }

    private void doScan(String dir) {
        File fi = new File(dir);
        File[] producers = fi.listFiles(pathname -> !pathname.getName().startsWith("."));
        assert producers != null;
        for (File producer : producers) {
            File[] buckets = producer.listFiles(pathname -> !pathname.getName().startsWith("."));
            assert buckets != null;
            for (File bucket : buckets) {
                File[] producerSegments = bucket.listFiles();
                assert producerSegments != null;
                Arrays.sort(producerSegments, Comparator.comparingInt(a -> Integer.parseInt(a.getName())));
                List<Path> bucketSegment = segments.computeIfAbsent(bucket.getName(), k -> new ArrayList<>());
                for (File segment : producerSegments) {
                    bucketSegment.add(segment.toPath());
                }
            }
        }
    }

    private List<Path> loadProducers(String dir, String bucket) {
        File fi = new File(dir);
        File[] producers = fi.listFiles(pathname -> !pathname.getName().startsWith("."));
        assert producers != null;
        List<Path> producerSegmentPath = new ArrayList<>(producers.length);
        for (File p : producers) {
            File bu = p.toPath().resolve(bucket).toFile();
            File[] producerSegments = bu.listFiles();
            assert producerSegments != null;
            Arrays.sort(producerSegments, Comparator.comparingInt(a -> Integer.parseInt(a.getName())));
            for (File segment : producerSegments) {
                producerSegmentPath.add(segment.toPath());
            }
        }
        return producerSegmentPath;
    }
}
