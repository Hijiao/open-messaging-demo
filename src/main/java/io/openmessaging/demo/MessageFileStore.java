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


    private Map<String, List<byte[]>> beforeWriteBody = new HashMap<>();

    private PageCacheWriteUnitQueueManager writeQueueManager = PageCacheWriteUnitQueueManager.getInstance();
    private PageCacheReadUnitQueueManager readUnitQueueManager = PageCacheReadUnitQueueManager.getInstance();


    DefaultProducer producer = new DefaultProducer();

    public void putMessage(boolean isTopic, String bucket, Message message) {
        synchronized (beforeWriteBody) {
            if (!beforeWriteBody.containsKey(bucket)) {
                beforeWriteBody.put(bucket, new ArrayList(Constants.SEND_TO_WRITE_QUEUE_BATCH_SIZE));
            }
        }

        allocateOnFileTableAndSendToWriteQueue(isTopic, bucket, (BytesMessage) message);
    }


    public void allocateOnFileTableAndSendToWriteQueue(boolean isTopic, String bucket, BytesMessage message) {
        List<byte[]> beforeWriteBodyList = beforeWriteBody.get(bucket);

        synchronized (beforeWriteBodyList) {
            beforeWriteBodyList.add(message.getBody());
            if (beforeWriteBodyList.size() > Constants.SEND_TO_WRITE_QUEUE_BATCH_SIZE) {
                PageCacheWriteUnitQueue queue = writeQueueManager.getBucketWriteQueue(bucket, isTopic);
                sendBatchMessageToQueue(beforeWriteBodyList, queue);

            }
        }

    }

    private void sendBatchMessageToQueue(List<byte[]> beforeWriteBodyList, PageCacheWriteUnitQueue queue) {
        try {
            for (int i = 0; i < beforeWriteBodyList.size(); i++) {
                byte[] body = beforeWriteBodyList.get(i);
                queue.productWriteBody(body);
            }
            beforeWriteBodyList.clear();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }



    public Message pullMessage(boolean isTopic, String bucket) {
        PageCacheReadUnitQueue readUnitQueue = readUnitQueueManager.getBucketReadUnitQueue(bucket, isTopic);
        byte[] body = null;
        try {
            body = readUnitQueue.consumeReadBody();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (body == null) {
            return null;
        } else {
            if (readUnitQueue.isTopic()) {
                return producer.createBytesMessageToTopic(bucket, body);
            }
            return producer.createBytesMessageToQueue(bucket, body);

        }

    }

    public synchronized void flushWriteBuffers() {
        for (Map.Entry<String, List<byte[]>> entry : beforeWriteBody.entrySet()) {
            //System.out.println("bucket:====== " + entry.getKey());
            List<byte[]> beforeWriteBodyList = entry.getValue();
            if (beforeWriteBodyList.isEmpty()) continue;
            PageCacheWriteUnitQueue queue = writeQueueManager.getBucketWriteQueue(entry.getKey(), true);
            sendBatchMessageToQueue(beforeWriteBodyList, queue);
        }
    }

}
