package io.openmessaging.demo;


import io.openmessaging.Message;

import java.util.ArrayList;
import java.util.List;
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


    private Map<String, List<DefaultBytesMessage>> beforeWriteBody = new ConcurrentHashMap<>();

    private PageCacheWriteUnitQueueManager writeQueueManager = PageCacheWriteUnitQueueManager.getInstance();
    private PageCacheReadUnitQueueManager readUnitQueueManager = PageCacheReadUnitQueueManager.getInstance();


    public void putMessage(boolean isTopic, String bucket, Message message) {
        synchronized (beforeWriteBody) {
            if (!beforeWriteBody.containsKey(bucket)) {
                beforeWriteBody.put(bucket, new ArrayList(Constants.SEND_TO_WRITE_QUEUE_BATCH_SIZE + 64));
            }
        }

        allocateOnFileTableAndSendToWriteQueue(isTopic, bucket, (DefaultBytesMessage) message);
    }


    public void allocateOnFileTableAndSendToWriteQueue(boolean isTopic, String bucket, DefaultBytesMessage message) {
        List<DefaultBytesMessage> beforeWriteBodyList = beforeWriteBody.get(bucket);
        synchronized (beforeWriteBodyList) {
            beforeWriteBodyList.add(message);
            if (beforeWriteBodyList.size() > Constants.SEND_TO_WRITE_QUEUE_BATCH_SIZE) {
                PageCacheWriteUnitQueue queue = writeQueueManager.getBucketWriteQueue(bucket, isTopic);
                sendBatchMessageToQueue(beforeWriteBodyList, queue);
            }
        }

    }

    private void sendBatchMessageToQueue(List<DefaultBytesMessage> beforeWriteBodyList, PageCacheWriteUnitQueue queue) {
        try {
            for (int i = 0; i < beforeWriteBodyList.size(); i++) {
                DefaultBytesMessage message = beforeWriteBodyList.get(i);
                queue.putMessageInWriteQueue(message);
            }
            beforeWriteBodyList.clear();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public Message pullMessage(boolean isTopic, String bucket) {
        PageCacheReadUnitQueue readUnitQueue = readUnitQueueManager.getBucketReadUnitQueue(bucket, isTopic);
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

        for (Map.Entry<String, List<DefaultBytesMessage>> entry : beforeWriteBody.entrySet()) {
            //System.out.println("bucket:====== " + entry.getKey());
            List<DefaultBytesMessage> beforeWriteBodyList = entry.getValue();
            PageCacheWriteRunner runner = writeRunnerMap.get(entry.getKey());
            if (beforeWriteBodyList.isEmpty()) {
                runner.getQueue().setFinish(true);
            } else {
                PageCacheWriteUnitQueue queue = runner.getQueue();
                sendBatchMessageToQueue(beforeWriteBodyList, queue);
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
        System.out.println("all write threads exit");

    }
}
