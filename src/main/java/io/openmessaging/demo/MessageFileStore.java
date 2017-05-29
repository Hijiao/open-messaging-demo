package io.openmessaging.demo;


import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Max on 2017/5/19.
 */
public class MessageFileStore {

    static Logger logger = LoggerFactory.getLogger(MessageFileStore.class);
    private static final MessageFileStore INSTANCE = new MessageFileStore();

    public static MessageFileStore getInstance() {
        return INSTANCE;
    }


    private Map<String, LinkedList<Integer>> messageBuckets = new HashMap<>();
    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();
    private Map<String, List<byte[]>> beforeWriteBody = new HashMap<>();

    private PageCacheWriteUnitQueueManager writeQueueManager = PageCacheWriteUnitQueueManager.getInstance();
    private PageCacheReadUnitQueueManager readUnitQueueManager = PageCacheReadUnitQueueManager.getInstance();
    private static AtomicInteger beforeWriteBodyCount = new AtomicInteger();


    DefaultProducer producer = new DefaultProducer();

    public void putMessage(boolean isTopic, String bucket, Message message) {
        LinkedList<Integer> bucketList;
        synchronized (messageBuckets) {
            if (!messageBuckets.containsKey(bucket)) {

                bucketList = new LinkedList<>();
                messageBuckets.put(bucket, bucketList);
                beforeWriteBody.put(bucket, new ArrayList(Constants.SEND_TO_WRITE_QUEUE_BATCH_SIZE));
            }
        }


        bucketList = messageBuckets.get(bucket);

//        while (bucketList == null) {
//            bucketList = messageBuckets.get(bucket);
//            if (bucketList != null) {
//                break;
//            } else {
//                bucketList = new LinkedList<>();
//                messageBuckets.put(bucket, bucketList);
//                beforeWriteBody.put(bucket, new ArrayList(Constants.SEND_TO_WRITE_QUEUE_BATCH_SIZE));
//            }
//        }
        allocateOnFileTableAndSendToWriteQueue(bucketList, isTopic, bucket, (BytesMessage) message);
    }


    public void allocateOnFileTableAndSendToWriteQueue(LinkedList<Integer> bucketList, boolean isTopic, String bucket, BytesMessage message) {
        List<byte[]> beforeWriteBodyList = beforeWriteBody.get(bucket);

        synchronized (beforeWriteBodyList) {
            beforeWriteBodyList.add(message.getBody());
            if (beforeWriteBodyList.size() > Constants.SEND_TO_WRITE_QUEUE_BATCH_SIZE) {
                PageCacheWriteUnitQueue queue = writeQueueManager.getBucketWriteQueue(bucket, isTopic);
                try {
                    for (int i = 0; i < beforeWriteBodyList.size(); i++) {
                        queue.productWriteBody(beforeWriteBodyList.get(i));
                    }
                    beforeWriteBodyList.clear();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }


        //bucketList.addLast(message.getBody().length);
        //writeQueueManager.getBucketWriteQueue(bucket, isTopic).productWriteBody(message.getBody());
        //return pageCacheManager.write(lastRecoder, isTopic, bucket, message);

    }

    public void showAllBuckets() {
        for (String bucket : messageBuckets.keySet()) {
            for (Integer record : messageBuckets.get(bucket)) {
                System.out.println("{bucket = " + bucket + ", len = " + record.toString());
            }
        }
    }


    public synchronized Message pullMessage(String filepath, boolean isTopic, String bucket) {
        LinkedList<Integer> bucketList = messageBuckets.get(bucket);
        PageCacheReadUnitQueue readUnitQueue = readUnitQueueManager.getBucketReadUnitQueue(bucketList, bucket, isTopic);
        byte[] body = readUnitQueue.consumeReadBody();
        if (body == null) {
            return null;
        } else {
            logger.debug("body: {}", body);
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
