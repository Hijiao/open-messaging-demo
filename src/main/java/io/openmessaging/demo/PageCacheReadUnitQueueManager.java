package io.openmessaging.demo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by Max on 2017/5/26.
 */
public class PageCacheReadUnitQueueManager {
    private static final PageCacheReadUnitQueueManager INSTANCE = new PageCacheReadUnitQueueManager();

    public static PageCacheReadUnitQueueManager getInstance() {
        return INSTANCE;
    }


    private static final Map<String, PageCacheReadUnitQueue> bucketsReadQueueMap = new ConcurrentHashMap<>();

    private static String filePath;

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }


    //使用：getBucketReadUnitQueue.consumeReadBody;
    public PageCacheReadUnitQueue getBucketReadUnitQueue(List lenList, String bucket, boolean isTopic) {
        synchronized (bucketsReadQueueMap) {
            PageCacheReadUnitQueue queue = bucketsReadQueueMap.get(bucket);
            if (queue == null) {
                queue = new PageCacheReadUnitQueue(isTopic);
                bucketsReadQueueMap.put(bucket, queue);
                PageCacheReadRunner runner = new PageCacheReadRunner(lenList, queue, bucket, filePath);
                ThreadPoolExecutor executor = PageCacheReadPoolManager.getThreadPool();
                executor.execute(runner);
            }
            return queue;

        }
    }

}
