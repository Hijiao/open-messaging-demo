package io.openmessaging.demo;

import io.openmessaging.BytesMessage;

import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheManager {


    public static final PageCacheManager INSTANCE = new PageCacheManager();
    private long messageFileRecordStartPos;
    private int messageFileRecordLenth;

    public static PageCacheManager getInstance() {
        return INSTANCE;
    }


    private HashMap<String, LinkedList<MappedByteBuffer>> bucketPageList = new HashMap<String, LinkedList<MappedByteBuffer>>();


    public MessageFileRecord write(MessageFileRecord lastRecode, boolean isTopic, String bucket, BytesMessage message) {
        messageFileRecordLenth = message.getBody().length;
        messageFileRecordStartPos = lastRecode.getRecord_pos();


    }


}
