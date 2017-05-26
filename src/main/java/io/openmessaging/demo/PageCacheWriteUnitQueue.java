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

    private LinkedBlockingQueue<PageCacheWriteUnit> queue = new LinkedBlockingQueue<>();

    public void producWriteUnit(PageCacheWriteUnit unit) {
        try {
            queue.put(unit);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public PageCacheWriteUnit consumeWriteUnit() {
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
