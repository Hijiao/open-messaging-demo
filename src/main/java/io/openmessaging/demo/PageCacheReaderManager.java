package io.openmessaging.demo;

import io.openmessaging.MessageHeader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

import static io.openmessaging.demo.Constants.*;
import static io.openmessaging.demo.PageCacheManager.unmap;

/**
 * Created by Max on 2017/6/2.
 */
public class PageCacheReaderManager extends Thread {


    private PageCacheReaderManager() {

    }

    private static PageCacheReaderManager INSTANCE = new PageCacheReaderManager();

    public static PageCacheReaderManager getInstance() {
        return INSTANCE;
    }


    MappedByteBuffer currPage = null;
    boolean hasPackagedOneMessage = false;
    boolean finishFlag = false;
    String storePath;
    int currPageNumber = -1;

    boolean isReadFinished = false;
    ByteBuffer tmpByteBuffer = ByteBuffer.allocate(Constants.BYTE_BUFFER_SIZE);


    public void setStorePath(String path) {
        storePath = path;
        randAccessFile = initRandomFile();
    }


    RandomAccessFile randAccessFile;


    private RandomAccessFile initRandomFile() {
        StringBuilder builder = new StringBuilder();
        builder.append(storePath).append(File.separator).append("message.txt");
        try {
            return new RandomAccessFile(new File(builder.toString()), "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void run() {
//        Map<String, DefaultBytesMessage> map = MessageMap.getInstance().getMap();
//        while (!isReadFinished) {
//
//            getMessageFromFileAndSentToMap(map);
//        }
        try {
            while (true) {
                DefaultBytesMessage message = getMessageFromFile();
                if (message != null) {
                    //System.out.println("               new message:"+message);
                    String bucketName = message.headers().getString(MessageHeader.TOPIC);
                    if (bucketName != null) {
                        PageCacheReadUnitQueueManager.getBucketReadUnitQueue(bucketName).put(message);
                    } else {
                        bucketName = message.headers().getString(MessageHeader.QUEUE);
                        PageCacheReadUnitQueueManager.getBucketReadUnitQueue(bucketName).put(message);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    private DefaultBytesMessage getMessageFromFile() {
        String key = null;
        DefaultBytesMessage message = new DefaultBytesMessage();

        hasPackagedOneMessage = false;
        if (currPage == null && currPageNumber == -1) {
            createNewPageToRead(++currPageNumber);
        }
        if (currPage == null) {
            isReadFinished = true;
            return null;
        }
        //int offset = getNextIntFromCurrPage();

        while ((!hasPackagedOneMessage) && (!finishFlag)) {
            byte currByte = getNextByteFromCurrPage();
            if (currByte != MARKER_PREFIX) {
                if (currByte == 0x00) {
                    return null;
                }
                tmpByteBuffer.put(currByte);
            } else {
                byte byteType = getNextByteFromCurrPage();
                switch (byteType) {
                    case HEADER_KEY_END_MARKER:
                        //tmpByteBuffer.flip();
                        key = new String(tmpByteBuffer.array(), 0, tmpByteBuffer.position());
                        break;
                    case HEADER_VALUE_END_MARKER:
                        message.putHeaders(key, new String(tmpByteBuffer.array(), 0, tmpByteBuffer.position()));
                        key = null;
                        break;
                    case PRO_KEY_END_MARKER:
                        key = new String(tmpByteBuffer.array(), 0, tmpByteBuffer.position());
                        break;
                    case PRO_VALUE_END_MARKER:
                        message.putProperties(key, new String(tmpByteBuffer.array(), 0, tmpByteBuffer.position()));
                        key = null;
                        break;
                    case MESSAGE_END_MARKER:
                        int bodyLen = tmpByteBuffer.position();
                        byte[] body = new byte[bodyLen];
                        System.arraycopy(tmpByteBuffer.array(), 0, body, 0, bodyLen);
                        tmpByteBuffer.rewind();
                        tmpByteBuffer.get(body);
                        message.setBody(body);
                        hasPackagedOneMessage = true;
                        body = null;
                        break;
                }
                tmpByteBuffer.rewind();

            }
        }
        if (hasPackagedOneMessage) {
            // System.out.println(message);
            return message;
        }

        return null;
    }


    private void getMessageFromFileAndSentToMap(Map<String, DefaultBytesMessage> map) {
        String key = null;
        DefaultBytesMessage message = new DefaultBytesMessage();

        hasPackagedOneMessage = false;
        if (currPage == null && currPageNumber == -1) {
            createNewPageToRead(++currPageNumber);
        }
        if (currPage == null) {
            isReadFinished = true;
            return;
        }
        int offset = getNextIntFromCurrPage();

        while ((!hasPackagedOneMessage) && (!finishFlag)) {
            byte currByte = getNextByteFromCurrPage();
            if (currByte != MARKER_PREFIX) {
                if (currByte == 0x00) {
                    return;
                }
                tmpByteBuffer.put(currByte);
            } else {
                byte byteType = getNextByteFromCurrPage();
                switch (byteType) {
                    case HEADER_KEY_END_MARKER:
                        //tmpByteBuffer.flip();
                        key = new String(tmpByteBuffer.array(), 0, tmpByteBuffer.position());
                        break;
                    case HEADER_VALUE_END_MARKER:
                        message.putHeaders(key, new String(tmpByteBuffer.array(), 0, tmpByteBuffer.position()));
                        key = null;
                        break;
                    case PRO_KEY_END_MARKER:
                        key = new String(tmpByteBuffer.array(), 0, tmpByteBuffer.position());
                        break;
                    case PRO_VALUE_END_MARKER:
                        message.putProperties(key, new String(tmpByteBuffer.array(), 0, tmpByteBuffer.position()));
                        key = null;
                        break;
                    case MESSAGE_END_MARKER:
                        int bodyLen = tmpByteBuffer.position();
                        byte[] body = new byte[bodyLen];
                        System.arraycopy(tmpByteBuffer.array(), 0, body, 0, bodyLen);
                        tmpByteBuffer.rewind();
                        tmpByteBuffer.get(body);
                        message.setBody(body);
                        hasPackagedOneMessage = true;
                        body = null;
                        break;
                }
            }
        }
        if (hasPackagedOneMessage) {
//            System.out.println(byteBuffer.toString());
            String bucketName;
            bucketName = message.headers().getString(MessageHeader.TOPIC);
            if (bucketName == null) {
                bucketName = message.headers().getString(MessageHeader.QUEUE);
            }
            map.put(bucketName + "#" + offset, message);
        }

    }


    private void createNewPageToRead(int index) {
        if (index != 0) {
            unmap(currPage);
            // closeCurrPage();
            currPage = null;
            System.gc();
        }

        try {
            currPage = randAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, Constants.MAPPED_BYTE_BUFF_PAGE_SIZE * index, Constants.MAPPED_BYTE_BUFF_PAGE_SIZE * (index + 1));
        } catch (Exception e) {
            //e.printStackTrace();
            try {
                currPage = randAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, Constants.MAPPED_BYTE_BUFF_PAGE_SIZE * index, randAccessFile.length() - Constants.MAPPED_BYTE_BUFF_PAGE_SIZE * index);
            } catch (IOException e1) {
                e1.printStackTrace();
            }

            currPage = null;
        }

    }

    private Integer getNextIntFromCurrPage() {

        int remain = currPage.remaining();
        if (remain > 4) {
            return currPage.getInt();
        }
        byte[] b = new byte[4];
        for (int i = 0; i < remain; i++) {
            b[i] = currPage.get();
        }
        createNewPageToRead(++currPageNumber);

        for (int i = remain; i < 4; i++) {
            b[i] = currPage.get();
        }


        return b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;

    }

    private byte getNextByteFromCurrPage() {

        if (currPage != null) {
            if (currPage.hasRemaining()) {
                return currPage.get();
            } else {
                createNewPageToRead(++currPageNumber);

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
