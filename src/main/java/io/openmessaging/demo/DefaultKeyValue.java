package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DefaultKeyValue implements KeyValue {

    private final Map<String, Object> kvs = new HashMap<>();


    @Override
    public KeyValue put(String key, int value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public int getInt(String key) {
        return (Integer) kvs.getOrDefault(key, 0);
    }

    @Override
    public long getLong(String key) {
        return (Long) kvs.getOrDefault(key, 0L);
    }

    @Override
    public double getDouble(String key) {
        return (Double) kvs.getOrDefault(key, 0.0d);
    }

    @Override
    public String getString(String key) {
        return (String) kvs.getOrDefault(key, null);
    }

    @Override
    public Set<String> keySet() {
        return kvs.keySet();
    }

    public Map getMap() {
        return kvs;
    }

    @Override
    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }

    @Override

    public String toString() {
        Iterator<Map.Entry<String, Object>> i = kvs.entrySet().iterator();
        if (!i.hasNext())
            return "";

        StringBuilder sb = new StringBuilder();


        for (; ; ) {
            Map.Entry<String, Object> e = i.next();
            String key = e.getKey();
            Object value = e.getValue();
            sb.append(key);
            sb.append('=');
            sb.append(value);
            if (!i.hasNext())
                return sb.toString();
            sb.append(',');
        }
    }

    public static void main(String[] args) {
        byte b = 's';
        long start = System.currentTimeMillis();
        byte[] test = new byte[2000000000];
        for (int i = 0; i < 2000000000; i++) {
            test[i] = (byte) i;
        }
        long end1 = System.currentTimeMillis();
        System.out.println(end1 - start);

        for (int i = 0; i < 2000000000; i++) {
            //test[i]=(byte)i;
            if (test[i] == 1) {

            } else if (test[i] == 2) {
                b = 1;
            } else if (test[i] == 3) {
                b = 2;
            } else if (test[i] == 4) {
                b = 3;
            } else if (test[i] == 12) {
                b = 1;
            } else if (test[i] == 13) {
                b = 2;
            } else if (test[i] == 14) {
                b = 3;
            } else if (test[i] == 222) {
                b = 1;
            } else if (test[i] == 23) {
                b = 2;
            } else if (test[i] == 24) {
                b = 3;
            }
        }

        long end2 = System.currentTimeMillis();

        System.out.println(end2 - end1);

    }
}
