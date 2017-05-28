package io.openmessaging.demo;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

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






    private int currPageNumber = 0;
    private MappedByteBuffer lastPage;
    MappedByteBuffer currPage;
    int currPageRemaining;





    public void writeByte(byte[] body) {
        if (lastPage == null) {
            lastPage = createNewPageToWirte(0);
        }
        currPageRemaining = lastPage.remaining();
        if (currPageRemaining < body.length) {
            for (int i = 0; i < currPageRemaining; i++) {
                lastPage.put(body[i]);
            }

            lastPage.put(body, 0, currPageRemaining);
            //flushAndCloseLastPage();

            lastPage = createNewPageToWirte(++currPageNumber);
            for (int i = currPageRemaining; i < body.length; i++) {
                lastPage.put(body[i]);
            }
            //for (int i =0;i<len;++i)
            //tarByteArray=lastPage.get();
        } else {
            for (int i = 0; i < body.length; i++) {
                lastPage.put(body[i]);
            }
        }

    }

    public byte[] readByte(int messageLen) {
        if (currPage == null) {
            currPage = createNewPageToRead(0);
        }

        byte[] body = new byte[messageLen];
        currPageRemaining = currPage.remaining();
        if (currPageRemaining < messageLen) {
            for (int l = 0; l < currPageRemaining; l++) {
                try {
                    body[l] = currPage.get();
                } catch (Exception e) {
                    System.out.println(e);
                }

            }
            currPage = createNewPageToRead(++currPageNumber);
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


    private MappedByteBuffer createNewPageToWirte(int index) {
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
            e.printStackTrace();
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


    public static void main(String[] args) {

        File commitLogFileHolder = new File("/Users/Max/code/tianchi/tmp/");
        File[] commitLogFiles = commitLogFileHolder.listFiles();
        for (File file : commitLogFiles) {
            System.out.println(file.getName());
        }
        System.out.println(commitLogFiles.length);
    }

}
