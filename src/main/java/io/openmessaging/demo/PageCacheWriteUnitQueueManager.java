package io.openmessaging.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheWriteUnitQueueManager {

    private static String filePath = null;

    private static final PageCacheWriteUnitQueueManager INSTANCE = new PageCacheWriteUnitQueueManager();

    public static PageCacheWriteUnitQueueManager getInstance() {
        return INSTANCE;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    //    private static final Map<String, PageCacheWriteUnitQueue> bucketsWriteQueueMap = new ConcurrentHashMap<>();
    private static final Map<String, PageCacheWriteUnitQueue> bucketsWriteQueueMap = new HashMap<>();

    private static final ThreadPoolExecutor executor = PageCacheWritePoolManager.getThreadPool();

    //使用： getBucketWriteQueue().productWriteUnit(writeUnit);
    public PageCacheWriteUnitQueue getBucketWriteQueue(String bucket, boolean isTopic) {
        synchronized (bucketsWriteQueueMap) {
            PageCacheWriteUnitQueue queue = bucketsWriteQueueMap.get(bucket);
            if (queue == null) {
                queue = new PageCacheWriteUnitQueue(isTopic);
                bucketsWriteQueueMap.put(bucket, queue);
                PageCacheWriteRunner runner = new PageCacheWriteRunner(bucketsWriteQueueMap.get(bucket), bucket, filePath);
                executor.execute(runner);
            }
        }
        return bucketsWriteQueueMap.get(bucket);
    }

}
