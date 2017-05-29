package io.openmessaging.demo;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheWriteRunner extends Thread {

    private PageCacheWriteUnitQueue queue;
    String queueBucketName;
    private PageCacheManager cacheManager;


    public PageCacheWriteRunner(PageCacheWriteUnitQueue queue, String queueBucketName, String storePath) {
        this.queue = queue;
        this.queueBucketName = queueBucketName;
        this.cacheManager = new PageCacheManager(queueBucketName, storePath);
        System.out.println("init new  write_thread");

    }

    public void run() {
        try {
            while (true) {
                cacheManager.writeByte(queue.consumeWriteBody());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        cacheManager.flushAndCloseLastPage();
    }

    public PageCacheManager getCacheManager() {
        return cacheManager;
    }
}
