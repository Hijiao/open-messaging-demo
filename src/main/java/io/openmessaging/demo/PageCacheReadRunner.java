package io.openmessaging.demo;


/**
 * Created by Max on 2017/5/28.
 */
public class PageCacheReadRunner extends Thread {
    private PageCacheReadUnitQueue queue;
    private PageCacheManager cacheManager;


    public PageCacheReadRunner(PageCacheReadUnitQueue queue, String queueBucketName, String storePath) {
        this.queue = queue;
        this.cacheManager = new PageCacheManager(queueBucketName, storePath);
    }

    public void run() {
        try {
            while (true) {
                byte[] body = cacheManager.readByte();
                if (body == null) {
                    queue.setFinish(true);
                    break;
                }
                queue.productReadBody(body);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //TODO 确定一下读写池不会占用太多内存（最好是 消费速度远大于生产速度）
    }
}


