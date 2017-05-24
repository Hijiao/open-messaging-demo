package io.openmessaging.demo;

import io.openmessaging.BytesMessage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheWriteUnitQueueManager {
    private static Map<String, PageCacheWriteUnitQueue> bucketsWriteQueueMap = new ConcurrentHashMap<>();

    //使用： getBucketWriteQueue().productWriteUnit(writeUnit);
    public static PageCacheWriteUnitQueue getBucketWriteQueue(String bucket) {
        PageCacheWriteUnitQueue queue = bucketsWriteQueueMap.get(bucket);
        if (queue == null) {
            queue = new PageCacheWriteUnitQueue();
            bucketsWriteQueueMap.put(bucket, queue);
            PageCacheWriteRunner runner = new PageCacheWriteRunner(queue);
            ThreadPoolExecutor executor = PageCacheWritePoolManager.getThreadPool();
            executor.execute(runner);
        }
        return queue;
    }
//

}
