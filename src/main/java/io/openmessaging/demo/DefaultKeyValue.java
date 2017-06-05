package io.openmessaging.demo;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import io.openmessaging.KeyValue;
import io.openmessaging.MessageHeader;

public class DefaultKeyValue implements KeyValue {

    private final Map<String, String> kvs = new HashMap<>();

    static DefaultKeyValue fromByte(ByteBuffer buf) {
        int kvNum = Util.byteToInt(buf);
        DefaultKeyValue keyValue = new DefaultKeyValue();
        while (kvNum > 0) {
            byte k = buf.get();
            String key = null;
            byte[] vb;
            int vl;
            String val;
            if (k != 0) {
                vl = Util.byteToInt(buf);
                vb = new byte[vl];
                buf.get(vb, 0, vl);
                val = new String(vb);
                switch (k) {
                    case 84:
                        key = MessageHeader.TOPIC;
                        val = "TOPIC_" + val;
                        break;
                    case 77:
                        key = MessageHeader.MESSAGE_ID;
                        break;
                    case 80:
                        key = "PRO_OFFSET";
                        val = "PRODUCER" + val;
                        break;
                    case 81:
                        key = MessageHeader.QUEUE;
                        val = "QUEUE_" + new String(vb);
                        break;
                    default:
                }
            } else {
                int kl = Util.byteToInt(buf);
                byte[] kb = new byte[kl];
                buf.get(kb, 0, kl);
                key = new String(kb);
                vl = Util.byteToInt(buf);
                vb = new byte[vl];
                buf.get(vb, 0, vl);
                val = new String(vb);
            }
            keyValue.kvs.put(key, val);
            kvNum--;
        }
        return keyValue;
    }

    static void writeTo(DefaultKeyValue kv,ByteBuffer buf) {
        byte[] kvNumSize = Util.intToByte(kv.kvs.size());
        buf.put(kvNumSize, 0, kvNumSize.length);

        Iterator<Map.Entry<String, String>> itr = kv.kvs.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, String> entry = itr.next();
            String k = entry.getKey();
            String v = entry.getValue();
            writeTo(k, v, buf);
        }
    }

    private static void writeTo(String k, String value, ByteBuffer buf) {
        switch (k) {
            case MessageHeader.TOPIC:
                value = value.substring(6);
                buf.put((byte) 'T');
                break;
            case MessageHeader.QUEUE:
                value = value.substring(6);
                buf.put((byte) 'Q');
                break;
            case MessageHeader.MESSAGE_ID:
                buf.put((byte) 'M');
                break;
            case "PRO_OFFSET":
                buf.put((byte) 'P');
                value = value.substring(8);
                break;
            default:
                buf.put((byte) 0);
                byte[] kb = k.getBytes();
                byte[] klb = Util.intToByte(kb.length);
                buf.put(klb, 0, klb.length);
                buf.put(kb, 0, kb.length);
        }
        byte[] vb = value.getBytes();
        byte[] vlb = Util.intToByte(vb.length);
        buf.put(vlb, 0, vlb.length);
        buf.put(vb, 0, vb.length);
    }

    @Override
    public KeyValue put(String key, int value) {
        kvs.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        kvs.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        kvs.put(key, String.valueOf(value));
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public int getInt(String key) {
        return Integer.parseInt(kvs.get(key));
    }

    @Override
    public long getLong(String key) {
        return Long.parseLong(kvs.get(key));
    }

    @Override
    public double getDouble(String key) {
        return Double.parseDouble(kvs.get(key));
    }

    @Override
    public String getString(String key) {
        return kvs.get(key);
    }

    @Override
    public Set<String> keySet() {
        return kvs.keySet();
    }

    @Override
    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }

    @Override
    public String toString() {
        return "DefaultKeyValue [kvs=" + kvs + "]";
    }
}
