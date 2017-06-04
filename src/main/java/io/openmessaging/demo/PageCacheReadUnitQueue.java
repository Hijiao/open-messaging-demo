package io.openmessaging.demo;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Max on 2017/5/26.
 */
public class PageCacheReadUnitQueue {

    private boolean isFinish = false;
    private String bucketName;
    // private DefaultBytesMessage message;

    public PageCacheReadUnitQueue(String bucketName) {
        //System.out.println("init PageCacheReadUnitQueue:" + bucketName);
        this.bucketName = bucketName;
    }

    private LinkedBlockingQueue<DefaultBytesMessage> queue = new LinkedBlockingQueue(Constants.READER_QUEUE_SIZE);

    public String getBucketName() {
        return bucketName;
    }

    /**
     * take()方法和put()方法是对应的，从中拿一个数据，如果拿不到线程挂起
     * poll()方法和offer()方法是对应的，从中拿一个数据，如果没有直接返回null
     */

    public void productReadBody(DefaultBytesMessage message) throws InterruptedException {
        queue.put(message);
        // queue.offer(messageBody);
    }

    public void offer(DefaultBytesMessage message) {
        queue.offer(message);
    }

    public void put(DefaultBytesMessage message) throws InterruptedException {
//        System.out.println(bucketName + " put one");

        queue.put(message);
    }

    public DefaultBytesMessage poll() {
//        System.out.println(bucketName + " poll one");
        synchronized (PageCacheReadUnitQueue.class) {
            return queue.poll();
        }
    }

    public DefaultBytesMessage take() {
        try {
//            System.out.println(bucketName + " take one");
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean isEmpty() {
        return queue.isEmpty();
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
//                Thread.sleep(1000);
            } else {
                return null;
            }
        }
        return queue.take();
//        if (isTopic) {
//            message.putHeaders(MessageHeader.TOPIC, bucketName);
//        } else {
//            message.putHeaders(MessageHeader.QUEUE, bucketName);
//        }
        //return message;
    }

    public void setFinish(boolean finish) {
        isFinish = finish;
    }

    @Override
    public String toString() {
        return "PageCacheReadUnitQueue{" +
                "isFinish=" + isFinish +
                ", bucketName='" + bucketName + '\'' +
                ", queue=" + queue +
                '}';
    }
}
