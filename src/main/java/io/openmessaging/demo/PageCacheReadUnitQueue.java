package io.openmessaging.demo;

import io.openmessaging.MessageHeader;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Max on 2017/5/26.
 */
public class PageCacheReadUnitQueue {

    private boolean isTopic;
    private boolean isFinish = false;
    private String bucketName;

    public PageCacheReadUnitQueue(String bucketName, boolean isTopic) {
        this.isTopic = isTopic;
        this.bucketName = bucketName;
    }

    private LinkedBlockingQueue<DefaultBytesMessage> queue = new LinkedBlockingQueue();


    public void productReadBody(DefaultBytesMessage message) throws InterruptedException {
        queue.put(message);
        // queue.offer(messageBody);
    }

    public DefaultBytesMessage consumeReadBody() throws InterruptedException {
//        if (queue.isEmpty()) {
//            return null;
//        } else return queue.poll();
//        if (isFinish) {
//            if (queue.isEmpty())
//                return null;
//        }

        while (queue.isEmpty()) {
            if (!isFinish) {
                Thread.sleep(100);
            } else {
                return null;
            }
        }
        DefaultBytesMessage message = queue.poll();
        if (isTopic) {
            message.putHeaders(MessageHeader.TOPIC, bucketName);
        } else {
            message.putHeaders(MessageHeader.QUEUE, bucketName);
        }
        return message;
    }

    public void setFinish(boolean finish) {
        isFinish = finish;
    }

    public boolean isTopic() {
        return isTopic;
    }
}
