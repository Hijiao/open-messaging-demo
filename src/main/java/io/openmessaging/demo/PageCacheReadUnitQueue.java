package io.openmessaging.demo;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Max on 2017/5/26.
 */
public class PageCacheReadUnitQueue {

    private boolean isTopic;

    public PageCacheReadUnitQueue(boolean isTopic) {
        this.isTopic = isTopic;
    }

    private ConcurrentLinkedQueue<byte[]> queue = new ConcurrentLinkedQueue();


    public void producWriteUnit(byte[] messageBody) {
        queue.offer(messageBody);
    }

    public byte[] consumeUnit() {

        if (queue.isEmpty()) {
            return null;
        } else return queue.poll();
    }

    public boolean isTopic() {
        return isTopic;
    }
}
