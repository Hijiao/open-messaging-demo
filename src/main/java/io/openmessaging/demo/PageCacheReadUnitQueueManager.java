package io.openmessaging.demo;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Max on 2017/5/26.
 */
public class PageCacheReadUnitQueueManager {
    private static final PageCacheReadUnitQueueManager INSTANCE = new PageCacheReadUnitQueueManager();

    public static PageCacheReadUnitQueueManager getInstance() {
        return INSTANCE;
    }


    private static final Map<String, PageCacheReadUnitQueue> bucketsReadQueueMap = new HashMap<>();

    private static String filePath;

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }


    //使用：getBucketReadUnitQueue.consumeReadBody;
    public PageCacheReadUnitQueue getBucketReadUnitQueue(String bucket, boolean isTopic) {
        synchronized (bucketsReadQueueMap) {
            PageCacheReadUnitQueue queue = bucketsReadQueueMap.get(bucket);
            if (queue == null) {
                queue = new PageCacheReadUnitQueue(bucket, isTopic);
                bucketsReadQueueMap.put(bucket, queue);
                PageCacheReadRunner runner = new PageCacheReadRunner(queue, bucket, filePath);
                runner.start();
//                ThreadPoolExecutor executor = PageCacheReadPoolManager.getThreadPool();
//                executor.execute(runner);
            }
            return queue;

        }
    }

}
