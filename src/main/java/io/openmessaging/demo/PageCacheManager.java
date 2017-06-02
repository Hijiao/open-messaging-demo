package io.openmessaging.demo;

import io.openmessaging.MessageHeader;

import java.io.File;
import java.io.RandomAccessFile;
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
public class PageCacheManager {

    //TODO 批量移动buffer http://www.cnblogs.com/lxzh/archive/2013/05/10/3071680.html

    MappedByteBuffer currPage = null;
    int currPageRemaining;
    boolean hasPackagedOneMessage = false;
    boolean finishFlag = false;
    private String storePath;
    private String bucket;
    private boolean isTopic;
    private ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 300);//消息大小最大256kb
    private byte[] byteArray = new byte[1024 * 256];
    private int currPageNumber = -1;


    public PageCacheManager(String bucket, String storePath, boolean isTopic) {
        this.bucket = bucket;
        this.storePath = storePath;
        this.isTopic = isTopic;
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

    public static void main(String[] args) {
        String ss = "a";
        Testclass t = new Testclass(ss);
        ss = "b";
        System.out.println(t.gets());


    }

    public void writeMessage(DefaultBytesMessage message) {
        String key;
        byteBuffer.clear();
        if (currPage == null) {
            currPage = createNewPageToWrite(++currPageNumber);
        }
        Set<Map.Entry<String, Object>> entrySet = ((DefaultKeyValue) message.headers()).getMap().entrySet();
        if (entrySet.size() != 1) {
            for (Map.Entry<String, Object> kv : entrySet) {
                key = kv.getKey();
                if ((!MessageHeader.TOPIC.equals(key)) && (!MessageHeader.QUEUE.equals(key))) {//MessageId
                    byteBuffer.put(key.getBytes());
                    byteBuffer.put(MARKET_PREFIX_HEADER_KEY);
                    byteBuffer.put(((String) kv.getValue()).getBytes());
                    byteBuffer.put(MARKET_PREFIX_HEADER_VALUE);
                }
            }
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
            unmap(currPage);
            currPage = null;
            System.gc();
//            closeCurrPage();
            currPage = createNewPageToWrite(++currPageNumber);
            System.gc();
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
        int pageSize = (isTopic ? BIG_WRITE_PAGE_SIZE : SMALL_WRITE_PAGE_SIZE);

        StringBuilder builder = new StringBuilder();
        builder.append(storePath).append(File.separator).append(bucket).append("_").append(isTopic).append("_").append(String.format("%03d", index));
        try {
            RandomAccessFile randAccessFile = new RandomAccessFile(new File(builder.toString()), "rw");
            MappedByteBuffer newPage = randAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, pageSize);
            return newPage;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
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
        unmap(this.currPage);
        currPage = null;
        try {
            super.finalize();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

}


