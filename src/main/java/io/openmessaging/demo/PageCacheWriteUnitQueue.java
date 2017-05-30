package io.openmessaging.demo;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheWriteUnitQueue extends Thread {

    public PageCacheWriteUnitQueue(boolean isTopic) {
        this.isTopic = isTopic;
    }

    private boolean isTopic;

    private LinkedBlockingQueue<DefaultBytesMessage> queue = new LinkedBlockingQueue<>();


    public void putMessageInWriteQueue(DefaultBytesMessage message) throws InterruptedException {
        queue.put(message);

    }

    public DefaultBytesMessage getMessageFromWriteQueue() throws InterruptedException {
            return queue.take();
    }

    public boolean isTopic() {
        return isTopic;
    }

    public void setTopic(boolean topic) {
        isTopic = topic;
    }
}
