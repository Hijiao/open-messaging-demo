package io.openmessaging.demo;

import java.util.List;

/**
 * Created by Max on 2017/5/28.
 */
public class PageCacheReadRunner extends Thread {
    private PageCacheReadUnitQueue queue;
    String queueBucketName;
    private PageCacheManager cacheManager;
    private List<Integer> lenList;


    public PageCacheReadRunner(List lenList, PageCacheReadUnitQueue queue, String queueBucketName, String storePath) {
        this.lenList = lenList;
        this.queue = queue;
        this.queueBucketName = queueBucketName;
        this.cacheManager = new PageCacheManager(queueBucketName, storePath);
    }

    public void run() {
        for (int i = 0; i < lenList.size(); i++) {
            byte[] body = cacheManager.readByte(lenList.get(i));
            queue.productReadBody(body);
        }
        //TODO 确定一下读写池不会占用太多内存（最好是 消费速度远大于生产速度）
    }
}


