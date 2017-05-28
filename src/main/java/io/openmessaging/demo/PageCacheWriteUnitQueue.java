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


    public void producWriteBody(byte[] body) {
        try {
            queue.put(body);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public byte[] consumeWriteBody() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean isTopic() {
        return isTopic;
    }

    public void setTopic(boolean topic) {
        isTopic = topic;
    }
}
