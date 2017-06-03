package io.openmessaging.demo;

/**
 * Created by Max on 2017/5/20.
 */
public class Constants {

    // public static final int PAGE_SIZE_BITE_COUNT = 29; //512M
//    public static final int PAGE_SIZE_BITE_COUNT = 26; //64M


    //    public static final int PAGE_SIZE = 1 << PAGE_SIZE_BITE_COUNT; //8M  COUNT=23
    public static final int BYTE_BUFFER_NUMBER_IN_QUEUE = 30;

    public static final int BYTE_BUFFER_SIZE = 1024 * 256;

    public static final int SMALL_WRITE_PAGE_SIZE = 1024 * 1024 * 128;

    public static final int BIG_WRITE_PAGE_SIZE = 1024 * 1024 * 800;

    public static final int MAPPED_BYTE_BUFF_PAGE_SIZE = 1024 * 1024 * 16;

    public static final int WRITE_QUEUE_SIZE = 1000;


    public static final int PAGE_CACHE_WRITE_CORE_POOL_SIZE = 120;//核心线程数

    public static final int PAGE_CACHE_WRITE_MAX_POOL_SIZE = 120;//最大线程数

    public static final int PAGE_CACHE_WRITE_KEEP_ALIVE_TIME = 60; //60s

    public static final byte MARKER_PREFIX = '#';

    public static final byte HEADER_KEY_END_MARKER = (byte) '=';

    public static final byte HEADER_VALUE_END_MARKER = (byte) ';';

    public static final byte PRO_KEY_END_MARKER = (byte) '-';

    public static final byte PRO_VALUE_END_MARKER = (byte) ':';

    public static final byte MESSAGE_END_MARKER = (byte) '%';

    public static final byte[] MARKET_PREFIX_HEADER_KEY = new byte[]{MARKER_PREFIX, HEADER_KEY_END_MARKER};

    public static final byte[] MARKET_PREFIX_HEADER_VALUE = new byte[]{MARKER_PREFIX, HEADER_VALUE_END_MARKER};

    public static final byte[] MARKET_PREFIX_PRO_KEY = new byte[]{MARKER_PREFIX, PRO_KEY_END_MARKER};

    public static final byte[] MARKET_PREFIX_PRO_VALUE = new byte[]{MARKER_PREFIX, PRO_VALUE_END_MARKER};

    public static final byte[] MARKET_PREFIX_MESSAGE_END = new byte[]{MARKER_PREFIX, MESSAGE_END_MARKER};


    //    public static final int getPageNumber(int pos) {
//        return (pos >> PAGE_SIZE_BITE_COUNT);
//    }
//
//    public static final int getPageOffset(int pos) {
//        return (pos & offsetCounterpart);
//    }
//
    public static void main(String[] args) {
        byte[] b = new byte[]{MARKER_PREFIX, HEADER_KEY_END_MARKER};
        System.out.println(new String(MARKET_PREFIX_HEADER_KEY));
    }

}
