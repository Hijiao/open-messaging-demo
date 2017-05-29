package io.openmessaging.demo;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Max on 2017/5/26.
 */
public class PageCacheReadUnitQueue {

    private boolean isTopic;
    private boolean isFinish = false;

    public PageCacheReadUnitQueue(boolean isTopic) {
        this.isTopic = isTopic;
    }

    private LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue();


    public void productReadBody(byte[] messageBody) throws InterruptedException {
        queue.put(messageBody);
        // queue.offer(messageBody);
    }

    public byte[] consumeReadBody() throws InterruptedException {
//        if (queue.isEmpty()) {
//            return null;
//        } else return queue.poll();
//        if (isFinish) {
//            if (queue.isEmpty())
//                return null;
//        }

        while (queue.isEmpty()) {
            if (isFinish) {
                return null;
            }
        }

//        return queue.take();
        return queue.poll();
    }

    public void setFinish(boolean finish) {
        isFinish = finish;
    }

    public boolean isTopic() {
        return isTopic;
    }
}
