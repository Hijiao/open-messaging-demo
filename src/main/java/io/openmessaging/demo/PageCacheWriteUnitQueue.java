package io.openmessaging.demo;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheWriteUnitQueue extends Thread {

    public PageCacheWriteUnitQueue(boolean isTopic) {
        this.isTopic = isTopic;
    }

    private boolean isTopic;

    private LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();


    public void productWriteBody(byte[] body) throws InterruptedException {
            queue.put(body);

    }

    public byte[] consumeWriteBody() throws InterruptedException {
            return queue.take();
    }

    public boolean isTopic() {
        return isTopic;
    }

    public void setTopic(boolean topic) {
        isTopic = topic;
    }
}
