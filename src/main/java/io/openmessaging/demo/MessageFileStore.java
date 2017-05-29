package io.openmessaging.demo;


import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.util.*;

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


    private Map<String, LinkedList<Integer>> messageBucketsIndexTable = new HashMap<>();
    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();
    private Map<String, List<byte[]>> beforeWriteBody = new HashMap<>();

    private PageCacheWriteUnitQueueManager writeQueueManager = PageCacheWriteUnitQueueManager.getInstance();
    private PageCacheReadUnitQueueManager readUnitQueueManager = PageCacheReadUnitQueueManager.getInstance();


    DefaultProducer producer = new DefaultProducer();

    public void putMessage(boolean isTopic, String bucket, Message message) {
        LinkedList<Integer> bucketList;
        synchronized (messageBucketsIndexTable) {
            if (!messageBucketsIndexTable.containsKey(bucket)) {
                bucketList = new LinkedList<>();
                messageBucketsIndexTable.put(bucket, bucketList);
                beforeWriteBody.put(bucket, new ArrayList(Constants.SEND_TO_WRITE_QUEUE_BATCH_SIZE));
            }
        }


        bucketList = messageBucketsIndexTable.get(bucket);

//        while (bucketList == null) {
//            bucketList = messageBucketsIndexTable.get(bucket);
//            if (bucketList != null) {
//                break;
//            } else {
//                bucketList = new LinkedList<>();
//                messageBucketsIndexTable.put(bucket, bucketList);
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
                        byte[] body = beforeWriteBodyList.get(i);
                        bucketList.addLast(body.length);
                        queue.productWriteBody(body);
                    }
                    beforeWriteBodyList.clear();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }


        //writeQueueManager.getBucketWriteQueue(bucket, isTopic).productWriteBody(message.getBody());
        //return pageCacheManager.write(lastRecoder, isTopic, bucket, message);

    }

    public void showAllBuckets() {
        for (String bucket : messageBucketsIndexTable.keySet()) {
            for (Integer record : messageBucketsIndexTable.get(bucket)) {
                System.out.println("{bucket = " + bucket + ", len = " + record.toString());
            }
        }
    }


    public Message pullMessage(boolean isTopic, String bucket) {
        LinkedList<Integer> bucketList = messageBucketsIndexTable.get(bucket);

        PageCacheReadUnitQueue readUnitQueue = readUnitQueueManager.getBucketReadUnitQueue(bucketList, bucket, isTopic);
        byte[] body = new byte[0];
        try {
            body = readUnitQueue.consumeReadBody();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (body == null) {
            return null;
        } else {
            logger.debug("body: {}", body);
            if (readUnitQueue.isTopic()) {
                return producer.createBytesMessageToTopic(bucket, body);
            }
            return producer.createBytesMessageToQueue(bucket, body);

        }

    }

    public synchronized void flushWriteBuffers() {
        for (Map.Entry<String, List<byte[]>> entry : beforeWriteBody.entrySet()) {
            System.out.println("bucket:====== " + entry.getKey());
            List<byte[]> beforeWriteBodyList = entry.getValue();
            if (beforeWriteBodyList.isEmpty()) continue;
            LinkedList<Integer> bucketList = messageBucketsIndexTable.get(entry.getKey());
            PageCacheWriteUnitQueue queue = writeQueueManager.getBucketWriteQueue(entry.getKey(), true);

            try {
                for (int i = 0; i < beforeWriteBodyList.size(); i++) {
                    byte[] body = beforeWriteBodyList.get(i);
                    bucketList.addLast(body.length);
                    queue.productWriteBody(body);
                }
                wireBucketsIndexTableToFile(entry.getKey(), bucketList);
                beforeWriteBodyList.clear();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    private void wireBucketsIndexTableToFile(String bucket, List<Integer> indexTable) {


    }
}
