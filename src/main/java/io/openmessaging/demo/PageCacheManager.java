package io.openmessaging.demo;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by Max on 2017/5/23.
 */
public class PageCacheManager {

    private String storePath;//TODO init
    private String bucket;

    public PageCacheManager(String bucket, String storePath) {
        this.bucket = bucket;
        this.storePath = storePath;
    }


    private int currPageNumber = -1;
    private MappedByteBuffer lastPage;
    MappedByteBuffer currPage;
    int currPageRemaining;


    public void writeByte(byte[] body) {

        if (lastPage == null) {
            lastPage = createNewPageToWrite(0);
        }
        currPageRemaining = lastPage.remaining();
        int bodyAndIndexLenth = body.length + 4;
        if (currPageRemaining < bodyAndIndexLenth) {
            byte[] bodyAndIndex = packageBody(body, bodyAndIndexLenth);
            for (int i = 0; i < currPageRemaining; i++) {
                lastPage.put(bodyAndIndex[i]);
            }
            flushAndCloseLastPage();
            lastPage = createNewPageToWrite(++currPageNumber);
            for (int i = currPageRemaining; i < bodyAndIndexLenth; i++) {
                lastPage.put(bodyAndIndex[i]);
            }
            //for (int i =0;i<len;++i)
            //tarByteArray=lastPage.get();
        } else {
            byte[] index = intToByteArray(body.length);
            lastPage.put(index[0]);
            lastPage.put(index[1]);
            lastPage.put(index[2]);
            lastPage.put(index[3]);
            for (int i = 0; i < body.length; i++) {
                lastPage.put(body[i]);
            }
        }

    }

    byte[] indexByte = new byte[4];

    public byte[] readByte() {
        if (currPage == null && currPageNumber == -1) {
            currPage = createNewPageToRead(++currPageNumber);
        }
        if (currPage == null) {
            return null;
        }
        currPageRemaining = currPage.remaining();

        if (currPageRemaining <= 4) {
            for (int i = 0; i < currPageRemaining; i++)
                indexByte[i] = currPage.get();
            currPage = createNewPageToRead(++currPageNumber);
            if (currPage == null) {
                return null;
            }
            for (int i = currPageRemaining; i < 4; i++) {
                indexByte[i] = currPage.get();
            }
        } else {
            for (int i = 0; i < 4; i++) {
                indexByte[i] = currPage.get();
            }
        }

        int messageLen = byteArrayToInt(indexByte);
        if (messageLen == 0) {
            return null;
        }
        byte[] body = new byte[messageLen];
        currPageRemaining = currPage.remaining();
        if (currPageRemaining < messageLen) {
            for (int i = 0; i < currPageRemaining; i++) {
                body[i] = currPage.get();
            }
            currPage = createNewPageToRead(++currPageNumber);
            if (currPage == null) {
                return null;
            }
            for (int l = currPageRemaining; l < messageLen; l++) {
                body[l] = currPage.get();
            }
        } else {
            for (int l = 0; l < messageLen; l++) {
                body[l] = currPage.get();
            }
        }
        return body;
    }


    private MappedByteBuffer createNewPageToWrite(int index) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(storePath).append(File.separator).append(bucket).append("_").append(String.format("%03d", index));
        try {
            RandomAccessFile randAccessFile = new RandomAccessFile(new File(buffer.toString()), "rw");
            MappedByteBuffer newPage = randAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Constants.PAGE_SIZE);
            return newPage;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private MappedByteBuffer createNewPageToRead(int index) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(storePath).append(File.separator).append(bucket).append("_").append(String.format("%03d", index));
        try {
            RandomAccessFile randAccessFile = new RandomAccessFile(new File(buffer.toString()), "r");
            MappedByteBuffer newPage = randAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, Constants.PAGE_SIZE);
            return newPage;
        } catch (Exception e) {
            //e.printStackTrace();
            return null;
        }
    }


    public void flushAndCloseLastPage() {
        try {
//            MappedByteBuffer page = bucketPageList.getLast();
//            page.force();
            Method getCleanerMethod = lastPage.getClass().getMethod("cleaner", new Class[0]);
            getCleanerMethod.setAccessible(true);
            sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(lastPage, new Object[0]);
            cleaner.clean();
            System.gc();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int byteArrayToInt(byte[] b) {
        return b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    public static byte[] intToByteArray(int a) {
        return new byte[]{
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    public static byte[] packageBody(byte[] body, int bodyAndIndexLenth) {
        byte[] newBody = new byte[bodyAndIndexLenth];
        int a = body.length;
        newBody[0] = (byte) ((a >> 24) & 0xFF);
        newBody[1] = (byte) ((a >> 16) & 0xFF);
        newBody[2] = (byte) ((a >> 8) & 0xFF);

        newBody[3] = (byte) (a & 0xFF);
        for (int i = 0; i < a; a++) {
            newBody[i + 4] = body[i];
        }
        return newBody;

    }


    public static void main(String[] args) {

        int int2 = 199999991;
        byte[] bytesInt = intToByteArray(int2);
        System.out.println("bytesInt=" + bytesInt);//bytesInt=[B@de6ced
        System.out.println("int ->byte len:" + bytesInt.length);
    }

}
