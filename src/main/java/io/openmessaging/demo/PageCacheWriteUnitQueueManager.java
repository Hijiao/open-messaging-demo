package io.openmessaging.demo;

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

    private static final Map<String, PageCacheWriteUnitQueue> bucketsWriteQueueMap = new ConcurrentHashMap<>();

    //使用： getBucketWriteQueue().productWriteUnit(writeUnit);
    public synchronized PageCacheWriteUnitQueue getBucketWriteQueue(String bucket, boolean isTopic) {

        PageCacheWriteUnitQueue queue = bucketsWriteQueueMap.get(bucket);
        if (queue == null) {
            queue = new PageCacheWriteUnitQueue(isTopic);
            bucketsWriteQueueMap.put(bucket, queue);
            PageCacheWriteRunner runner = new PageCacheWriteRunner(queue, bucket, filePath);
            ThreadPoolExecutor executor = PageCacheWritePoolManager.getThreadPool();
            executor.execute(runner);
        }
        return queue;
    }
//

}
