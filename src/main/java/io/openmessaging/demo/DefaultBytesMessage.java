package io.openmessaging.demo;

import java.nio.ByteBuffer;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

public class DefaultBytesMessage implements BytesMessage {

    private DefaultKeyValue headers = new DefaultKeyValue();

    private DefaultKeyValue properties = new DefaultKeyValue();
    private byte[] body;

    DefaultBytesMessage(byte[] body) {
        this.body = body;
    }

    static DefaultBytesMessage fromByte(ByteBuffer buffer) {
        int len = Util.byteToInt(buffer);
        if (len == 0) {
            return null;
        }
        byte[] body = new byte[len];
        buffer.get(body, 0, len);
        DefaultKeyValue headers = DefaultKeyValue.fromByte(buffer);
        DefaultKeyValue properties = DefaultKeyValue.fromByte(buffer);
        DefaultBytesMessage msg = new DefaultBytesMessage(body);
        msg.headers = headers;
        msg.properties = properties;
        return msg;
    }

    static boolean writeTo(DefaultBytesMessage message, ByteBuffer buf) {
        int pos=buf.position();
        try {
            int bodyLen = message.body.length;
            byte[] blb = Util.intToByte(bodyLen);
            buf.put(blb, 0, blb.length);
            buf.put(message.body, 0, bodyLen);
            DefaultKeyValue.writeTo(message.headers, buf);
            DefaultKeyValue.writeTo(message.properties, buf);
        }catch (Exception e){
            buf.position(pos);
            return false;
        }
        return true;
    }

    @Override
    public byte[] getBody() {
        return body;
    }

    @Override
    public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override
    public KeyValue headers() {
        return headers;
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, int value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, long value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, double value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, String value) {
        properties.put(key, value);
        return this;
    }
}
