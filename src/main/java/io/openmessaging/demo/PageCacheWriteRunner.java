package io.openmessaging.demo;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheWriteRunner extends Thread {

    private PageCacheWriteUnitQueue queue;

    public PageCacheWriteRunner(PageCacheWriteUnitQueue queue) {
        this.queue = queue;
    }

    public void run() {
        try {
            while (true) {
                queue.consumeWriteUnit();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
