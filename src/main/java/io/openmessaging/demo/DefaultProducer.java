package io.openmessaging.demo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;

public class DefaultProducer implements Producer {
    private static AtomicInteger PRODUCER_ID_IDX = new AtomicInteger(0);
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private KeyValue properties;
    private Map<String, BucketWriter> writers = new HashMap<>();
    private String dir;
    private int producerId;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        this.dir = properties.getString("STORE_PATH");
        this.producerId = PRODUCER_ID_IDX.getAndIncrement();
    }

    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public void send(Message message) {
        String bucket = message.headers().getString(MessageHeader.TOPIC);
        if (bucket == null) {
            bucket = message.headers().getString(MessageHeader.QUEUE);
        }
        BucketWriter bw = writers.computeIfAbsent(bucket, b -> new BucketWriter(dir, b, producerId));
        bw.append((DefaultBytesMessage) message);
    }

    @Override
    public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void flush() {
        Iterator<Map.Entry<String, BucketWriter>> itr = writers.entrySet().iterator();
        while (itr.hasNext()) {
            itr.next().getValue().flush();
        }
    }
}
