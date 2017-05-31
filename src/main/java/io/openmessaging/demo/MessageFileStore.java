package io.openmessaging.demo;


import io.openmessaging.Message;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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


    public synchronized void putMessage(boolean isTopic, String bucket, Message message) {

        if (!beforeWriteBodyMap.containsKey(bucket)) {
            beforeWriteBodyMap.put(bucket, new ConcurrentLinkedQueue<>());
            writeQueueManager.intBucketWriteQueue(bucket, isTopic);
        }
        Queue<DefaultBytesMessage> beforeWriteBodyQueue = beforeWriteBodyMap.get(bucket);
        beforeWriteBodyQueue.add((DefaultBytesMessage) message);
        if (beforeWriteBodyQueue.size() > Constants.SEND_TO_WRITE_QUEUE_BATCH_SIZE) {
            PageCacheWriteUnitQueue queue = writeQueueManager.getBucketWriteQueue(bucket);
            sendBatchMessageToQueue(beforeWriteBodyQueue, queue);
        }

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

    boolean hasFlushed = false;

    public synchronized void flushWriteBuffers() {
        if (hasFlushed) {
            return;
        } else {
            hasFlushed = true;
        }
        Map<String, PageCacheWriteRunner> writeRunnerMap = writeQueueManager.getBucketsWriteThreadMap();

        for (Map.Entry<String, Queue<DefaultBytesMessage>> entry : beforeWriteBodyMap.entrySet()) {
            //System.out.println("bucket:====== " + entry.getKey());
            Queue<DefaultBytesMessage> beforeWriteBodyQueue = entry.getValue();
            PageCacheWriteRunner runner = writeRunnerMap.get(entry.getKey());
            if (beforeWriteBodyQueue.isEmpty()) {
                runner.getQueue().setFinish(true);
            } else {

                //TODO 当总数小于批次数时，queue一直未被初始化，flush会报错
                PageCacheWriteUnitQueue queue = writeQueueManager.getBucketWriteQueue(entry.getKey());
                // PageCacheWriteUnitQueue queue = runner.getQueue();
                sendBatchMessageToQueue(beforeWriteBodyQueue, queue);
                queue.setFinish(true);
            }

        }
        try {

            for (PageCacheWriteRunner writeRunner : writeRunnerMap.values()) {
                writeRunner.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("all write threads  exit");

    }
}
