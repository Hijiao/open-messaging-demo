package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.MessageHeader;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Set;

import static io.openmessaging.demo.Constants.*;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheManager extends Thread {

    //TODO 批量移动buffer http://www.cnblogs.com/lxzh/archive/2013/05/10/3071680.html

    private PageCacheManager() {

    }

    public static final PageCacheManager INSTANCE = new PageCacheManager();

    public static PageCacheManager getInstance() {
        return INSTANCE;
    }

    public void setStorePath(String path) {
        storePath = path;
        initRandomFile();
    }


    PageCacheWriteUnitQueue writeQueue = PageCacheWriteUnitQueueManager.getWriteQueue();

    MappedByteBuffer currPage = null;
    int currPageRemaining;
    private String storePath;
    private ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 300);//消息大小最大256kb
    private int currPageNumber = -1;
    RandomAccessFile randAccessFile;
    DefaultKeyValue bucketsOffsetMap = new DefaultKeyValue();


    @Override
    public void run() {
        try {
            while (true) {
                DefaultBytesMessage message = writeQueue.take();
                if (message != null) {
                    String bucketName = message.headers().getString(MessageHeader.TOPIC);
                    if (bucketName == null) {
                        bucketName = message.headers().getString(MessageHeader.QUEUE);
                    }
                    int offset = bucketsOffsetMap.getInt(bucketName);
                    bucketsOffsetMap.put(bucketName, ++offset);
                    writeMessage(offset, message);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void flushOffsetToFile() {
        StringBuilder builder = new StringBuilder();
        builder.append(storePath).append(File.separator).append("offset.txt");
        try {
            FileWriter fw = new FileWriter(builder.toString());
            fw.write(bucketsOffsetMap.toString());
            //System.out.println(bucketsOffsetMap.toString());
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        currPage.force();
    }

    public PageCacheManager(String bucket, String storePath, boolean isTopic) {
        this.storePath = storePath;
        initRandomFile();
    }

    private void initRandomFile() {
        StringBuilder builder = new StringBuilder();
        builder.append(storePath).append(File.separator).append("message.txt");
        try {
            randAccessFile = new RandomAccessFile(new File(builder.toString()), "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void unmap(final MappedByteBuffer mappedByteBuffer) {
        try {
            if (mappedByteBuffer == null) {
                return;
            }
            //mappedByteBuffer.force();
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                @SuppressWarnings("restriction")
                public Object run() {
                    try {
                        Method getCleanerMethod = mappedByteBuffer.getClass().getMethod("cleaner", new Class[0]);
                        getCleanerMethod.setAccessible(true);
                        sun.misc.Cleaner cleaner =
                                (sun.misc.Cleaner) getCleanerMethod.invoke(mappedByteBuffer, new Object[0]);
                        cleaner.clean();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void writeMessage(Integer offset, DefaultBytesMessage message) {
        String key;
        byteBuffer.clear();
        //byteBuffer.putInt(offset);
        if (currPage == null) {
            currPage = createNewPageToWrite(++currPageNumber);
        }
        Set<Map.Entry<String, Object>> entrySet = ((DefaultKeyValue) message.headers()).getMap().entrySet();

        for (Map.Entry<String, Object> kv : entrySet) {
            key = kv.getKey();
            byteBuffer.put(key.getBytes());
            byteBuffer.put(MARKET_PREFIX_HEADER_KEY);
            byteBuffer.put(((String) kv.getValue()).getBytes());
            byteBuffer.put(MARKET_PREFIX_HEADER_VALUE);

        }

        DefaultKeyValue properties = (DefaultKeyValue) message.properties();

        if (properties != null) {
            entrySet = properties.getMap().entrySet();
            for (Map.Entry<String, Object> kv : entrySet) {
                key = kv.getKey();
                byteBuffer.put(key.getBytes());
                byteBuffer.put(MARKET_PREFIX_PRO_KEY);
                byteBuffer.put(((String) kv.getValue()).getBytes());
                byteBuffer.put(MARKET_PREFIX_PRO_VALUE);
            }
        }

        byteBuffer.put(message.getBody());
        byteBuffer.put(MARKET_PREFIX_MESSAGE_END);

        currPageRemaining = currPage.remaining();
        int messageLen = byteBuffer.position();
        byteBuffer.rewind();
        if (messageLen >= currPageRemaining) {
            for (int i = 0; i < currPageRemaining; i++) {
                currPage.put(byteBuffer.get());
            }
            currPage.force();
            unmap(currPage);
            currPage = null;
            System.gc();
//            closeCurrPage();
            currPage = createNewPageToWrite(++currPageNumber);
            for (int i = currPageRemaining; i < messageLen; i++) {
                currPage.put(byteBuffer.get());
            }
        } else {
            for (int i = 0; i < messageLen; i++) {
                currPage.put(byteBuffer.get());
            }
        }

    }


    private MappedByteBuffer createNewPageToWrite(int index) {
        try {
            return randAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, Constants.MAPPED_BYTE_BUFF_PAGE_SIZE * index, Constants.MAPPED_BYTE_BUFF_PAGE_SIZE * (index + 1));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


//    public void closeCurrPage() {
//        try {
////            MappedByteBuffer page = bucketPageList.getLast();
////            page.force();
//
//            Method getCleanerMethod = currPage.getClass().getMethod("cleaner", new Class[0]);
//            getCleanerMethod.setAccessible(true);
//            sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(currPage, new Object[0]);
//            cleaner.clean();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    static class Testclass {
        private String s;

        public Testclass(String sz) {
            s = sz;
        }

        public String gets() {
            return s;
        }

    }

    protected void finalize() {
        currPage.force();
        unmap(this.currPage);
        currPage = null;
        try {
            randAccessFile.close();
            super.finalize();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    public static void main(String[] args) {
        KeyValue map = new DefaultKeyValue();
        map.put("a", 1);
        map.put("a", 2);

        System.out.println(map);
    }
}


