package io.openmessaging.demo;

/**
 * Created by Max on 2017/5/20.
 */
public class Constants {

    public static final int PAGE_SIZE_BITE_COUNT = 24; //16M

    public static final int PAGE_SIZE = 1 << PAGE_SIZE_BITE_COUNT; //8M  COUNT=23

    public static final int offsetCounterpart = PAGE_SIZE - 1;



    public static final int PAGE_CACHE_WRITE_CORE_POOL_SIZE = 20;//核心线程数

    public static final int PAGE_CACHE_WRITE_MAX_POOL_SIZE = 30;//最大线程数

    public static final int PAGE_CACHE_WRITE_KEEP_ALIVE_TIME = 60; //60s

    public static final int getPageNumber(int pos) {
        return (pos >> PAGE_SIZE_BITE_COUNT);
    }

    public static final int getPageOffset(int pos) {
        return (pos & offsetCounterpart);
    }

    public static void main(String[] args) {
        int pos = PAGE_SIZE;
        System.out.println("pageSize: " + PAGE_SIZE);
        System.out.println("pageNumber: " + getPageNumber(pos));
        System.out.println("offset: " + getPageOffset(pos));
    }

}
