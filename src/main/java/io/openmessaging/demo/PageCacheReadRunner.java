package io.openmessaging.demo;


import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.demo.Constants.*;

/**
 * Created by Max on 2017/5/28.
 */
public class PageCacheReadRunner extends Thread {
    private PageCacheReadUnitQueue queue;
    private PageCacheReaderManager cacheManager;
    private LinkedBlockingQueue<ByteBuffer> emptyByteBuffers = null;
    private LinkedBlockingQueue<ByteBuffer> fullByteBuffers = null;
    private String queueBucketName;
    private boolean isTopic;

    private byte[] byteArray = new byte[Constants.BYTE_BUFFER_SIZE];


    public PageCacheReadRunner(PageCacheReadUnitQueue queue, String queueBucketName, String storePath) {
        this.isTopic = queue.isTopic();
        this.queue = queue;
        this.queueBucketName = queueBucketName;
        this.cacheManager = new PageCacheReaderManager(queueBucketName, storePath, queue.isTopic());
        this.emptyByteBuffers = cacheManager.getEmptyByteBuffers();
        this.fullByteBuffers = cacheManager.getFullByteBuffers();
        System.out.println("init new read_thread： " + queueBucketName);
        Thread.currentThread().setName(queueBucketName);
        cacheManager.start();
//        cacheManager.isInterrupted();
    }

    public void run() {
        try {
            while (true) {
                while (fullByteBuffers.isEmpty()) {
                    if (cacheManager.isAlive()) {
//                Thread.sleep(1000);
                    } else {
                        queue.setFinish(true);
                        break;
                    }
                }
                ByteBuffer byteBuffer = fullByteBuffers.take();
                DefaultBytesMessage message = readMessageFromByteBuffer(byteBuffer);
                byteBuffer.clear();
                emptyByteBuffers.put(byteBuffer);
                queue.productReadBody(message);
            }

//            while (message != null) {
//                queue.productReadBody(message);
//                message = cacheManager.readMessageFromFileToByteBuffer();
//            }
            // queue.setFinish(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //TODO 确定一下读写池不会占用太多内存（最好是 消费速度远大于生产速度）
    }


    public DefaultBytesMessage readMessageFromByteBuffer(ByteBuffer byteBuffer) {
//        System.out.println("byteBuffer in readMessageFromByteBuffer" + byteBuffer.toString());

        int byteArrayLen = 0;

        String key = null;

        //TODO 拼接message头的工作在这里完成
        DefaultBytesMessage message = DefaultMessageFactory.createByteMessage(queueBucketName, isTopic);
        boolean hasPackagedOneMessage = false;
        byteBuffer.rewind();

        while ((!hasPackagedOneMessage) && (byteBuffer.hasRemaining())) {
            byte currByte = byteBuffer.get();
//            System.out.print(new String(new byte[]{currByte}));
            if (currByte != MARKER_PREFIX) {
                byteArray[byteArrayLen++] = currByte;
            } else {
                byte byteType = byteBuffer.get();
//                System.out.print(new String(new byte[]{byteType}));
                switch (byteType) {
                    case HEADER_KEY_END_MARKER:
                        key = new String(byteArray, 0, byteArrayLen);
                        break;
                    case HEADER_VALUE_END_MARKER:
                        message.putHeaders(key, new String(byteArray, 0, byteArrayLen));
                        key = null;
                        break;
                    case PRO_KEY_END_MARKER:
                        key = new String(byteArray, 0, byteArrayLen);
                        break;
                    case PRO_VALUE_END_MARKER:
                        message.putProperties(key, new String(byteArray, 0, byteArrayLen));
                        key = null;
                        break;
                    case MESSAGE_END_MARKER:
                        message.setBody(new String(byteArray, 0, byteArrayLen).getBytes());
                        hasPackagedOneMessage = true;
                        break;
                }
                byteArrayLen = 0;
            }
        }
        if (hasPackagedOneMessage) {
            // System.out.println("message"+message);
            return message;
        }
        return null;
    }

}


