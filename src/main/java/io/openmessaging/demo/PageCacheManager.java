package io.openmessaging.demo;

import io.openmessaging.MessageHeader;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Set;

import static io.openmessaging.demo.Constants.*;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheManager {

    //TODO 批量移动buffer http://www.cnblogs.com/lxzh/archive/2013/05/10/3071680.html

    private String storePath;
    private String bucket;
    private boolean isTopic;

    public PageCacheManager(String bucket, String storePath, boolean isTopic) {
        this.bucket = bucket;
        this.storePath = storePath;
        this.isTopic = isTopic;
    }

    private ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 300);//消息大小最大256kb


    private int currPageNumber = -1;
    MappedByteBuffer currPage = null;
    int currPageRemaining;


    String key;
    String value;
    byte[] body;
    Set<Map.Entry<String, Object>> entrySet;

    public void writeMessage(DefaultBytesMessage message) {
        byteBuffer.clear();
        if (currPage == null) {
            currPage = createNewPageToWrite(++currPageNumber);
        }
        entrySet = ((DefaultKeyValue) message.headers()).getMap().entrySet();
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
            closeCurrPage();
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


    private byte getNextByteFromCurrPage() {

        if (currPage != null) {
            if (currPage.hasRemaining()) {
                return currPage.get();
            } else {
                closeCurrPage();
                currPage = createNewPageToRead(++currPageNumber);
                System.gc();
            }
        }
        if (currPage == null)
            return MARKER_PREFIX;//连着返回两次，则认为文件读取结束
        return currPage.get();
    }

    boolean hasPackagedOneMessage = false;
    boolean finishFlag = false;

    public DefaultBytesMessage readMessage() {
        hasPackagedOneMessage = false;

        if (currPage == null && currPageNumber == -1) {
            currPage = createNewPageToRead(++currPageNumber);
        }
        if (currPage == null) {
            return null;
        }
        byteBuffer.clear();
        currPageRemaining = currPage.remaining();
        byte currByte;
        byte[] keyBytes;


        DefaultBytesMessage message = new DefaultBytesMessage();

        while ((!hasPackagedOneMessage) && (!finishFlag)) {
            currByte = getNextByteFromCurrPage();
            if (currByte != MARKER_PREFIX) {
                if (currByte == 0x00) {
                    return null;
                }
                byteBuffer.put(currByte);
            } else {
                switch (getNextByteFromCurrPage()) {
                    case HEADER_KEY_END_MARKER:
                        keyBytes = new byte[byteBuffer.position()];
                        byteBuffer.rewind();
                        byteBuffer.get(keyBytes);
                        key = new String(keyBytes);
                        break;
                    case HEADER_VALUE_END_MARKER:
                        keyBytes = new byte[byteBuffer.position()];
                        byteBuffer.rewind();
                        byteBuffer.get(keyBytes);
                        value = new String(keyBytes);
                        message.putHeaders(key, value);
                        break;
                    case PRO_KEY_END_MARKER:
                        keyBytes = new byte[byteBuffer.position()];
                        byteBuffer.rewind();
                        byteBuffer.get(keyBytes);
                        key = new String(keyBytes);
                        break;
                    case PRO_VALUE_END_MARKER:
                        keyBytes = new byte[byteBuffer.position()];
                        byteBuffer.rewind();
                        byteBuffer.get(keyBytes);
                        value = new String(keyBytes);
                        message.putProperties(key, value);
                        break;
                    case MESSAGE_END_MARKER:
                        body = new byte[byteBuffer.position()];
                        byteBuffer.rewind();
                        byteBuffer.get(body);
                        message.setBody(body);
                        hasPackagedOneMessage = true;
                        break;
                    case MARKER_PREFIX:
                        finishFlag = true;
                        break;
                }
                byteBuffer.clear();
            }
        }
        if (hasPackagedOneMessage) {
            return message;
        }
        return null;
    }


    private MappedByteBuffer createNewPageToWrite(int index) {
        int pageSize = (isTopic ? BIG_WRITE_PAGE_SIZE : SMALL_WRITE_PAGE_SIZE);

        StringBuilder builder = new StringBuilder();
        builder.append(storePath).append(File.separator).append(bucket).append("_").append(String.format("%03d", index));
        try {
            RandomAccessFile randAccessFile = new RandomAccessFile(new File(builder.toString()), "rw");
            MappedByteBuffer newPage = randAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, pageSize);
            return newPage;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private MappedByteBuffer createNewPageToRead(int index) {
        StringBuilder builder = new StringBuilder();
        int pageSize = (isTopic ? BIG_WRITE_PAGE_SIZE : SMALL_WRITE_PAGE_SIZE);
        builder.append(storePath).append(File.separator).append(bucket).append("_").append(String.format("%03d", index));
        try {
            RandomAccessFile randAccessFile = new RandomAccessFile(new File(builder.toString()), "r");
            MappedByteBuffer newPage = randAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, pageSize);
            return newPage;
        } catch (Exception e) {
            //e.printStackTrace();
            return null;
        }
    }


    public void closeCurrPage() {
        try {
//            MappedByteBuffer page = bucketPageList.getLast();
//            page.force();

            Method getCleanerMethod = currPage.getClass().getMethod("cleaner", new Class[0]);
            getCleanerMethod.setAccessible(true);
            sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(currPage, new Object[0]);
            cleaner.clean();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}


