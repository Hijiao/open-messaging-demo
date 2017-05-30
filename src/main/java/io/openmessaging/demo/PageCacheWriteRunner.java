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
        this.cacheManager = new PageCacheManager(queueBucketName, storePath, queue.isTopic());
        System.out.println("init new  write_thread：" + queueBucketName);

    }

    public void run() {
        try {
            while (true) {
                cacheManager.writeMessage(queue.getMessageFromWriteQueue());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        cacheManager.closeCurrPage();
    }

    public PageCacheManager getCacheManager() {
        return cacheManager;
    }

    public PageCacheWriteUnitQueue getQueue() {
        return queue;
    }
}
