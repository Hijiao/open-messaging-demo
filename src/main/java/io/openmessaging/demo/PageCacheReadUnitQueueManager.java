package io.openmessaging.demo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Max on 2017/5/26.
 */
public class PageCacheReadUnitQueueManager {
    private static final PageCacheReadUnitQueueManager INSTANCE = new PageCacheReadUnitQueueManager();

    public static PageCacheReadUnitQueueManager getInstance() {
        return INSTANCE;
    }


    private static final Map<String, PageCacheReadUnitQueue> bucketsReadQueueMap = new ConcurrentHashMap<>();

    private static final void loadCacheReadUnitQueue() {


    }


    //使用：getBucketReadUnitQueue.consumeUnit;
    public PageCacheReadUnitQueue getBucketReadUnitQueue(String bucket) {
        return bucketsReadQueueMap.get(bucket);
    }

}
