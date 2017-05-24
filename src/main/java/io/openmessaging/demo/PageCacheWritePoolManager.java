package io.openmessaging.demo;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by Max on 2017/5/24.
 */
public class PageCacheWritePoolManager {

    private static ThreadPoolExecutor threadPool = null;

    public static ThreadPoolExecutor getThreadPool() {
        if (threadPool == null) {
            threadPool = new ThreadPoolExecutor(
                    Constants.PAGE_CACHE_WRITE_CORE_POOL_SIZE,
                    Constants.PAGE_CACHE_WRITE_MAX_POOL_SIZE,
                    Constants.PAGE_CACHE_WRITE_KEEP_ALIVE_TIME,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<Runnable>(128),
                    new ThreadPoolExecutor.DiscardOldestPolicy());
        }
        return threadPool;
    }
}
