package io.openmessaging.demo;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheWriteUnitQueueManager {


    private PageCacheWriteUnitQueueManager() {


    }

    public static void initShips() {
        for (int i = 0; i < Constants.BWRITE_SHIP_NUMBER; i++) {
            emptyShips.offer(true);
        }
    }

    private static final PageCacheWriteUnitQueueManager INSTANCE = new PageCacheWriteUnitQueueManager();

    public static PageCacheWriteUnitQueueManager getInstance() {
        return INSTANCE;
    }

    //private static final ThreadPoolExecutor executor = PageCacheWritePoolManager.getThreadPool();

    //    private static final Map<String, PageCacheWriteUnitQueue> bucketsWriteQueueMap = new ConcurrentHashMap<>();
    private static final Map<String, PageCacheWriteUnitQueue> bucketsWriteQueueMap = new ConcurrentHashMap<>(120);

    private static final PageCacheWriteUnitQueue writeQueue = new PageCacheWriteUnitQueue();

    private static final LinkedBlockingQueue<Boolean> emptyShips = new LinkedBlockingQueue<>();

    private static LinkedBlockingQueue<ArrayList<DefaultBytesMessage>> loadedShips = new LinkedBlockingQueue<>();

    public static PageCacheWriteUnitQueue getWriteQueue() {
        return writeQueue;
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


    public static LinkedBlockingQueue<Boolean> getEmptyShips() {
        return emptyShips;
    }


    public static LinkedBlockingQueue<ArrayList<DefaultBytesMessage>> getLoadedShips() {
        return loadedShips;
    }

}
