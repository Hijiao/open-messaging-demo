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

    public PageCacheManager(String bucket, String storePath) {
        this.bucket = bucket;
        this.storePath = storePath;
    }

    private String storePath;//TODO init
    private String bucket;


    private LinkedList<MappedByteBuffer> bucketPageList = new LinkedList<>();

    private int currPageNumber;
    private MappedByteBuffer lastPage;
    private int endPos;


    public void write(PageCacheWriteUnit unit) {
        if (bucketPageList.isEmpty()) {
            createNewPageToWirte(0);
        }
        lastPage = bucketPageList.getLast();

        int messageFileRecordLength = unit.getMessage().getBody().length;
        long messageFileRecordStartPos = unit.getRecord().getRecord_pos();

        endPos = (int) messageFileRecordStartPos + messageFileRecordLength - 1;

        //if (Constants.getPageNumber(endPos) > currPageNumber) {
        int stop_pos = lastPage.remaining();
        if (stop_pos < messageFileRecordLength) {
            //write to last page fist,then clean it
//            int stop_pos=Constants.PAGE_SIZE-Constants.getPageOffset(endPos);
            try {
                lastPage.put(unit.getMessage().getBody(), 0, stop_pos);
            } catch (Exception e) {
                System.out.println(e.getStackTrace());
            }
            flushAndCloseLastPage();

            lastPage = createNewPageToWirte(++currPageNumber);

            lastPage.put(unit.getMessage().getBody(), stop_pos, messageFileRecordLength - stop_pos);
        } else {
            lastPage.put(unit.getMessage().getBody());
        }

    }


    private MappedByteBuffer createNewPageToWirte(int index) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(storePath).append(File.separator).append(bucket).append("_").append(index);
        try {
            RandomAccessFile randAccessFile = new RandomAccessFile(new File(buffer.toString()), "rw");
            MappedByteBuffer newPage = randAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Constants.PAGE_SIZE);
            bucketPageList.add(newPage);
            return newPage;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void flushAndCloseLastPage() {
        try {
            if (bucketPageList.isEmpty()) {
                return;
            }
            MappedByteBuffer page = bucketPageList.getLast();
            page.force();
            Method getCleanerMethod = page.getClass().getMethod("cleaner", new Class[0]);
            getCleanerMethod.setAccessible(true);
            sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(page, new Object[0]);
            cleaner.clean();
            System.gc();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
