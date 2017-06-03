package io.openmessaging.demo;


import io.openmessaging.Message;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Max on 2017/5/19.
 */
public class MessageFileStore {

    private static final MessageFileStore INSTANCE = new MessageFileStore();

    public static MessageFileStore getInstance() {
        return INSTANCE;
    }


    private Map<String, Queue<DefaultBytesMessage>> beforeWriteBodyMap = new ConcurrentHashMap<>();

    private PageCacheWriteUnitQueueManager writeQueueManager = PageCacheWriteUnitQueueManager.getInstance();
    private PageCacheReadUnitQueueManager readUnitQueueManager = PageCacheReadUnitQueueManager.getInstance();


    public void putMessage(Message message) {
//
//        if (!beforeWriteBodyMap.containsKey(bucket)) {
//            synchronized (beforeWriteBodyMap) {
//                if (!beforeWriteBodyMap.containsKey(bucket)) {
//                    beforeWriteBodyMap.put(bucket, new ConcurrentLinkedQueue<>());
//                    writeQueueManager.intBucketWriteQueue(bucket, isTopic);
//                }
//            }
//
//        }
////        Queue<DefaultBytesMessage> beforeWriteBodyQueue = beforeWriteBodyMap.get(bucket);
////        beforeWriteBodyQueue.add((DefaultBytesMessage) message);
//        PageCacheWriteUnitQueue queue = writeQueueManager.getBucketWriteQueue(bucket);

        PageCacheWriteUnitQueue queue = PageCacheWriteUnitQueueManager.getWriteQueue();
        queue.offer((DefaultBytesMessage) message);

    }


    private void sendBatchMessageToQueue(Queue<DefaultBytesMessage> beforeWriteBodyQueue, PageCacheWriteUnitQueue queue) {
        try {
            while (!beforeWriteBodyQueue.isEmpty()) {
                queue.putMessageInWriteQueue(beforeWriteBodyQueue.poll());
            }

            beforeWriteBodyQueue.clear();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public Message pullMessage(String bucket) {
        PageCacheReadUnitQueue readUnitQueue = readUnitQueueManager.getBucketReadUnitQueue(bucket);
        DefaultBytesMessage message = null;
        try {
            message = readUnitQueue.consumeReadBody();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return message;
    }


    public synchronized void flushWriteBuffers() {

        PageCacheWriteUnitQueue queue = PageCacheWriteUnitQueueManager.getWriteQueue();

        try {
            while (!queue.isEmpty()) {
                Thread.sleep(10);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.gc();


    }
}
