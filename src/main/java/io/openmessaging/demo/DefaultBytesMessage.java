package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.util.Arrays;

public class DefaultBytesMessage implements BytesMessage {

    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties;
    private byte[] body;

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }

    public DefaultBytesMessage() {
        this.body = body;
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
//        System.out.println("put int_value to headers: " + key + "=" + value);
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, long value) {
//        System.out.println("put long_value to headers: " + key + "=" + value);

        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, double value) {
//        System.out.println("put double_value to headers: " +key + "=" +  value);
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, String value) {
//        System.out.println("put string_value to headers: " + key + "=" + value);

        headers.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        System.out.println("put string_value to pro: " + key + "=" + value);
        return this;
    }

    @Override
    public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        return "DefaultBytesMessage{" +
                "headers={" + headers +
                "}, properties={" + properties +
                "}, body={" + Arrays.toString(body) +
                "}}";
    }
}
