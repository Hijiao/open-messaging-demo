package io.openmessaging.demo;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Max on 2017/6/3.
 */
public class MessageMap {
    private static MessageMap INSTANCE = new MessageMap();

    public static MessageMap getInstance() {
        return INSTANCE;
    }

    private Map<String, DefaultBytesMessage> map = new HashMap<>();

    public DefaultBytesMessage getMessage(String bucketsNameAndOffset) {
        return map.remove(bucketsNameAndOffset);
    }

    public Map<String, DefaultBytesMessage> getMap() {
        return map;
    }
}
