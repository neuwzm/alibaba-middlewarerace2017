package io.openmessaging.demo;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

public class DefaultPullConsumer implements PullConsumer {
    private KeyValue properties;
    private List<BucketReader> readers = new LinkedList<>();

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message poll() {
        Iterator<BucketReader> reader = readers.iterator();
        while (reader.hasNext()) {
            Message msg = reader.next().poll();
            if (msg != null) {
                return msg;
            }
            reader.remove();
        }
        return null;
    }


    @Override
    public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void attachQueue(String queueName, Collection<String> topics) {
        String dir = properties.getString("STORE_PATH");
        readers.add(new BucketReader(dir, queueName));
        for (String topic : topics) {
            readers.add(new BucketReader(dir, topic));
        }
    }

}
