package io.openmessaging.demo;


import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

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

    private PageCacheWriteUnitQueueManager writeQueueManager = PageCacheWriteUnitQueueManager.getInstance();
    private PageCacheReadUnitQueueManager readUnitQueueManager = PageCacheReadUnitQueueManager.getInstance();


    MessageFileRecord newRecoder;
    MessageFileRecord lastRecoder;

    DefaultProducer producer = new DefaultProducer();

    public synchronized void putMessage(boolean isTopic, String filePath, String bucket, Message message) {
        if (!messageBuckets.containsKey(bucket)) {
            messageBuckets.put(bucket, new LinkedList<>());
        }
        LinkedList<MessageFileRecord> bucketList = messageBuckets.get(bucket);
        allocateOnFileTableAndSendToWriteQueue(bucketList, isTopic, filePath, bucket, (BytesMessage) message);
    }


    public MessageFileRecord allocateOnFileTableAndSendToWriteQueue(LinkedList<MessageFileRecord> bucketList, boolean isTopic, String filePath, String bucket, BytesMessage message) {
        if (bucketList.isEmpty()) {
            newRecoder = new MessageFileRecord(0, message.getBody().length);
            bucketList.addLast(newRecoder);
            lastRecoder = newRecoder;
        } else {
            newRecoder = new MessageFileRecord(lastRecoder.getRecord_pos() + message.getBody().length, message.getBody().length);
            lastRecoder = newRecoder;
        }
        writeQueueManager.getBucketWriteQueue(bucket, isTopic).producWriteUnit(new PageCacheWriteUnit(newRecoder, message));
        //return pageCacheManager.write(lastRecoder, isTopic, bucket, message);
        return null;

    }

    public void showAllBuckets() {
        for (String bucket : messageBuckets.keySet()) {
            for (MessageFileRecord record : messageBuckets.get(bucket)) {
                System.out.println("{file=" + bucket + ", " + record.toString());
            }
        }
    }


    public synchronized Message pullMessage(String filepath, String queue, String bucket) {
        PageCacheReadUnitQueue readUnitQueue = readUnitQueueManager.getBucketReadUnitQueue(bucket);
        byte[] body = readUnitQueue.consumeUnit();
        if (body == null) {
            return null;
        } else {
            if (readUnitQueue.isTopic()) {
                return producer.createBytesMessageToTopic(bucket, body);
            }
            return producer.createBytesMessageToQueue(bucket, body);

        }




//        LinkedList<MessageFileRecord> fileRecords = messageBuckets.get(bucket);
//        if (fileRecords == null) {
//            return null;
//        }
//        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
//        if (offsetMap == null) {
//            offsetMap = new HashMap<>();
//            queueOffsets.put(queue, offsetMap);
//        }
//        int offset = offsetMap.getOrDefault(bucket, 0);
//        if (offset >= fileRecords.size()) {
//            return null;
//        }
//
//        MessageFileRecord fileRecord = fileRecords.get(offset);
//        byte[] messageBody = reader.read(filepath, bucket, fileRecord);
//
//        offsetMap.put(bucket, ++offset);
//        if (fileRecord.isTopic()) {
//            return producer.createBytesMessageToTopic(bucket, messageBody);
//        } else {
//            return producer.createBytesMessageToQueue(bucket, messageBody);
//        }
    }

}
