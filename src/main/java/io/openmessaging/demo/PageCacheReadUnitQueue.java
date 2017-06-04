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
        this.bucketName = bucketName;
    }

    private LinkedBlockingQueue<DefaultBytesMessage> queue = new LinkedBlockingQueue(Constants.BYTE_BUFFER_NUMBER_IN_QUEUE);


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
        queue.put(message);
    }

    public DefaultBytesMessage poll() {
        return queue.poll();
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

}
