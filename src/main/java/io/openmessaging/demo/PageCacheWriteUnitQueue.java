package io.openmessaging.demo;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheWriteUnitQueue {

    public PageCacheWriteUnitQueue(boolean isTopic) {
        this.isTopic = isTopic;
    }

    private boolean isTopic;
    private boolean isFinish = false;

    private LinkedBlockingQueue<DefaultBytesMessage> queue = new LinkedBlockingQueue<>();


    public void putMessageInWriteQueue(DefaultBytesMessage message) throws InterruptedException {
        queue.put(message);
        // queue.offer(message);
    }

    /**
     * take()方法和put()方法是对应的，从中拿一个数据，如果拿不到线程挂起
     * poll()方法和offer()方法是对应的，从中拿一个数据，如果没有直接返回null
     */

    public DefaultBytesMessage getMessageFromWriteQueue() throws InterruptedException {
        //return queue.take();
        while (queue.isEmpty()) {
            if (!isFinish) {
                Thread.sleep(10);
            } else {
                return null;
            }
        }
        return queue.poll();
    }

    public boolean isTopic() {
        return this.isTopic;
    }

    public void setTopic(boolean topic) {
        isTopic = topic;
    }

    public void setFinish(boolean finish) {
        isFinish = finish;
    }
}
