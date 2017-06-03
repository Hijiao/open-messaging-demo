package io.openmessaging.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheWriteUnitQueueManager {


    private static final PageCacheWriteUnitQueueManager INSTANCE = new PageCacheWriteUnitQueueManager();

    public static PageCacheWriteUnitQueueManager getInstance() {
        return INSTANCE;
    }

    //private static final ThreadPoolExecutor executor = PageCacheWritePoolManager.getThreadPool();

    //    private static final Map<String, PageCacheWriteUnitQueue> bucketsWriteQueueMap = new ConcurrentHashMap<>();
    private static final Map<String, PageCacheWriteUnitQueue> bucketsWriteQueueMap = new ConcurrentHashMap<>(120);
    private static final Map<String, PageCacheWriteRunner> bucketsWriteThreadMap = new HashMap<>(120);

    private static final PageCacheWriteUnitQueue writeQueue = new PageCacheWriteUnitQueue();

    public static PageCacheWriteUnitQueue getWriteQueue() {
        return writeQueue;
    }

    public Map<String, PageCacheWriteRunner> getBucketsWriteThreadMap() {
        return bucketsWriteThreadMap;
    }


//    private static final ThreadPoolExecutor executor = PageCacheWritePoolManager.getThreadPool();


    public static synchronized void intBucketWriteQueue(String bucket, boolean isTopic) {
        PageCacheWriteUnitQueue queue = new PageCacheWriteUnitQueue(isTopic);
        bucketsWriteQueueMap.put(bucket, queue);
//        PageCacheWriteRunner runner = new PageCacheWriteRunner(queue, bucket, filePath);
//        bucketsWriteThreadMap.put(bucket, runner);
        //executor.execute(runner);
//        runner.start();
    }

    //使用： getBucketWriteQueue().productWriteUnit(writeUnit);
    public static PageCacheWriteUnitQueue getBucketWriteQueue(String bucket) {
//        synchronized (bucketsWriteThreadMap) {
        //   PageCacheWriteRunner runner = bucketsWriteThreadMap.get(bucket);
//            if (runner == null) {
//                // System.out.println("create new writeUnitQueue :" + bucket + "  isTopic:" + isTopic);
//                PageCacheWriteUnitQueue queue = new PageCacheWriteUnitQueue(isTopic);
//                runner = new PageCacheWriteRunner(queue, bucket, filePath);
//                bucketsWriteThreadMap.put(bucket, runner);
//                //executor.execute(runner);
//                runner.start();
//            }
//            PageCacheWriteUnitQueue queue = bucketsWriteQueueMap.get(bucket);
//            if (queue == null) {
//                queue = new PageCacheWriteUnitQueue(isTopic);
//
//                bucketsWriteQueueMap.put(bucket, queue);
//                PageCacheWriteRunner runner = new PageCacheWriteRunner(bucketsWriteQueueMap.get(bucket), bucket, filePath);
//
//                executor.execute(runner);
//            }

        //  return runner.getQueue();
//        }
        return bucketsWriteQueueMap.get(bucket);
    }
}
