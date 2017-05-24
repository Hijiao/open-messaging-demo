package io.openmessaging.demo;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheWriteUnitQueue extends Thread {
    private LinkedBlockingQueue<PageCacheWriteUnit> queue = new LinkedBlockingQueue<>();

    public void producWriteUnit(PageCacheWriteUnit unit) throws InterruptedException {
        queue.put(unit);
    }

    public PageCacheWriteUnit consumeWriteUnit() throws InterruptedException {
        return queue.take();
    }


}
