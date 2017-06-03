package io.openmessaging.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.demo.Constants.*;
import static io.openmessaging.demo.PageCacheManager.unmap;

/**
 * Created by Max on 2017/6/2.
 */
public class PageCacheReaderManager extends Thread {


    MappedByteBuffer currPage = null;
    int currPageRemaining;
    boolean hasPackagedOneMessage = false;
    boolean finishFlag = false;
    private String storePath;
    private String bucket;
    private boolean isTopic;
    private int currPageNumber = -1;

    private boolean isReadFinished = false;

    private LinkedBlockingQueue<ByteBuffer> emptyByteBuffers = new LinkedBlockingQueue<>(BYTE_BUFFER_NUMBER_IN_QUEUE);
    private LinkedBlockingQueue<ByteBuffer> fullByteBuffers = new LinkedBlockingQueue<>(BYTE_BUFFER_NUMBER_IN_QUEUE);

    public LinkedBlockingQueue<ByteBuffer> getEmptyByteBuffers() {
        return emptyByteBuffers;
    }

    public LinkedBlockingQueue<ByteBuffer> getFullByteBuffers() {
        return fullByteBuffers;
    }

    RandomAccessFile randAccessFile;

    public PageCacheReaderManager(String bucket, String storePath, boolean isTopic) {
        this.bucket = bucket;
        this.storePath = storePath;
        this.isTopic = isTopic;
        for (int i = 0; i < BYTE_BUFFER_NUMBER_IN_QUEUE; i++) {
            try {
                emptyByteBuffers.put(ByteBuffer.allocateDirect(BYTE_BUFFER_SIZE));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        randAccessFile = initRandomFile();

    }

    private RandomAccessFile initRandomFile() {
        StringBuilder builder = new StringBuilder();
        builder.append(storePath).append(File.separator).append(bucket);
        try {
            return new RandomAccessFile(new File(builder.toString()), "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void run() {
        try {
            while (true) {
                ByteBuffer byteBuffer = emptyByteBuffers.take();
                ByteBuffer fullByteBuffer = readMessageFromFileToByteBuffer(byteBuffer);
                if (fullByteBuffer == null) {
                    isReadFinished = true;
                    break;
                }
                fullByteBuffers.put(fullByteBuffer);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    public ByteBuffer readMessageFromFileToByteBuffer(ByteBuffer byteBuffer) {

        //TODO 拼接message头的工作在这里完成
        hasPackagedOneMessage = false;
        if (currPage == null && currPageNumber == -1) {
            currPage = createNewPageToRead(++currPageNumber);
        }
        if (currPage == null) {
            return null;
        }
        currPageRemaining = currPage.remaining();
        while ((!hasPackagedOneMessage) && (!finishFlag)) {
            byte currByte = getNextByteFromCurrPage();
            byteBuffer.put(currByte);
            if (currByte != MARKER_PREFIX) {
                if (currByte == 0x00) {
                    return null;
                }
            } else {
                byte byteType = getNextByteFromCurrPage();
                byteBuffer.put(byteType);
                switch (byteType) {
                    case MESSAGE_END_MARKER:
                        hasPackagedOneMessage = true;
                        break;
                    case MARKER_PREFIX:
                        finishFlag = true;
                        break;
                }
            }
        }
        if (hasPackagedOneMessage) {
//            System.out.println(byteBuffer.toString());
            return byteBuffer;
        }
        return null;
    }


    private MappedByteBuffer createNewPageToRead(int index) {
        try {
            MappedByteBuffer byteBuffer = randAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, Constants.MAPPED_BYTE_BUFF_PAGE_SIZE * index, Constants.MAPPED_BYTE_BUFF_PAGE_SIZE * (index + 1));
            return byteBuffer;
        } catch (Exception e) {
            //e.printStackTrace();
            return null;
        }

    }

    private byte getNextByteFromCurrPage() {

        if (currPage != null) {
            if (currPage.hasRemaining()) {
                return currPage.get();
            } else {
                currPage.force();
                unmap(currPage);
                // closeCurrPage();
                currPage = null;
                System.gc();
                currPage = createNewPageToRead(++currPageNumber);

            }
        }
        if (currPage == null)
            return MARKER_PREFIX;//连着返回两次，则认为文件读取结束
        return currPage.get();
    }

    public boolean isReadFinished() {
        return isReadFinished;
    }
}
