package io.openmessaging.demo;


import io.openmessaging.BytesMessage;
import io.openmessaging.Message;
import io.openmessaging.Producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Max on 2017/5/19.
 */
public class MessageFileStore {
    private static final MessageFileStore INSTANCE = new MessageFileStore();

    public static MessageFileStore getInstance() {
        return INSTANCE;
    }

    private Map<String, LinkedList<MessageFileRecord>> messageBuckets = new ConcurrentHashMap<>();
    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();
    PageCacheManager pageCacheManager = PageCacheManager.getInstance();


    private MessageFileWriter write = new MessageFileWriter();

    private MessageFileReader reader = new MessageFileReader();

    private Producer producer = new DefaultProducer(null);


    public synchronized void putMessage(boolean isTopic, String filePath, String bucket, Message message) {
        if (!messageBuckets.containsKey(bucket)) {
            messageBuckets.put(bucket, new LinkedList<>());
        }
        LinkedList<MessageFileRecord> bucketList = messageBuckets.get(bucket);
        allocateOnFileTableAndSendToWriteQueue(bucketList, isTopic, filePath, bucket, (BytesMessage) message);
    }

    public MessageFileRecord allocateOnFileTableAndSendToWriteQueue(LinkedList<MessageFileRecord> bucketList, boolean isTopic, String filePath, String bucket, BytesMessage message) {
        MessageFileRecord lastRecoder = bucketList.getLast();
        return pageCacheManager.write(lastRecoder, isTopic, bucket, message);

    }

    public void showAllBuckets() {
        for (String bucket : messageBuckets.keySet()) {
            for (MessageFileRecord record : messageBuckets.get(bucket)) {
                System.out.println("{file=" + bucket + ", " + record.toString());
            }
        }
    }


    public synchronized Message pullMessage(String filepath, String queue, String bucket) {
        ArrayList<MessageFileRecord> fileRecords = messageBuckets.get(bucket);
        if (fileRecords == null) {
            return null;
        }
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset >= fileRecords.size()) {
            return null;
        }

        MessageFileRecord fileRecord = fileRecords.get(offset);
        byte[] messageBody = reader.read(filepath, bucket, fileRecord);

        offsetMap.put(bucket, ++offset);
        if (fileRecord.isTopic()) {
            return producer.createBytesMessageToTopic(bucket, messageBody);
        } else {
            return producer.createBytesMessageToQueue(bucket, messageBody);
        }

    }

}
