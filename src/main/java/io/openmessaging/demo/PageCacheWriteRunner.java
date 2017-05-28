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
    }

    public void run() {
        while (true) {
            cacheManager.writeByte(queue.consumeWriteBody());
        }
    }
}
