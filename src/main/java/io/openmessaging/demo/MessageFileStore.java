package io.openmessaging.demo;


import io.openmessaging.Message;

import java.util.List;

/**
 * Created by Max on 2017/5/19.
 */
public class MessageFileStore {

    private static final MessageFileStore INSTANCE = new MessageFileStore();

    public static MessageFileStore getInstance() {
        return INSTANCE;
    }


    private static PageCacheWriteUnitQueue queue = PageCacheWriteUnitQueueManager.getWriteQueue();


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

        queue.put((DefaultBytesMessage) message);

    }

    public void putAllMessages(List<DefaultBytesMessage> messages) {
        queue.addAll(messages);
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
