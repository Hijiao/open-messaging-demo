package io.openmessaging.demo;


import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

import static io.openmessaging.demo.Constants.*;

/**
 * Created by Max on 2017/5/28.
 */
public class PageCacheReadRunner extends Thread {
    PageCacheReadUnitQueue queue;
    PageCacheReaderManager pageCacheReaderManager;
    LinkedBlockingQueue<ByteBuffer> emptyByteBuffers = null;
    LinkedBlockingQueue<ByteBuffer> fullByteBuffers = null;
    String queueBucketName;
    boolean isTopic;
    //此处没法用allocateDirectory。。。
    ByteBuffer tmpByteBuffer = ByteBuffer.allocate(Constants.BYTE_BUFFER_SIZE);



    public PageCacheReadRunner(PageCacheReadUnitQueue queue, String queueBucketName, String storePath) {
        isTopic = queue.isTopic();
        this.queue = queue;
        this.queueBucketName = queueBucketName;
        this.pageCacheReaderManager = new PageCacheReaderManager(queueBucketName, storePath, queue.isTopic());
        this.emptyByteBuffers = pageCacheReaderManager.getEmptyByteBuffers();
        this.fullByteBuffers = pageCacheReaderManager.getFullByteBuffers();
        //System.out.println("init new read_thread： " + queueBucketName);
        //  Thread.currentThread().setName(queueBucketName);
        pageCacheReaderManager.start();
//        pageCacheReaderManager.isInterrupted();
    }

    public void run() {
        try {
            while (true) {
                while (fullByteBuffers.isEmpty()) {
                    if (pageCacheReaderManager.isAlive()) {
//                Thread.sleep(1000);
                    } else {
                        queue.setFinish(true);
                        this.queue = null;
                        this.pageCacheReaderManager = null;
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
//                message = pageCacheReaderManager.readMessageFromFileToByteBuffer();
//            }
            // queue.setFinish(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //TODO 确定一下读写池不会占用太多内存（最好是 消费速度远大于生产速度）
    }


    public DefaultBytesMessage readMessageFromByteBuffer(ByteBuffer byteBuffer) {
//        System.out.println("byteBuffer in readMessageFromByteBuffer" + byteBuffer.toString());


        String key = null;

        //TODO 拼接message头的工作在这里完成
        DefaultBytesMessage message = DefaultMessageFactory.createByteMessage(queueBucketName, isTopic);
        boolean hasPackagedOneMessage = false;
        byteBuffer.rewind();

        while ((!hasPackagedOneMessage) && (byteBuffer.hasRemaining())) {

            byte currByte = byteBuffer.get();
//            System.out.print(new String(new byte[]{currByte}));
            if (currByte != MARKER_PREFIX) {
                tmpByteBuffer.put(currByte);
                //byteArray[byteArrayLen++] = currByte;
            } else {
                byte byteType = byteBuffer.get();
//                System.out.print(new String(new byte[]{byteType}));
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
                tmpByteBuffer.clear();
            }
        }

        if (hasPackagedOneMessage) {
            // System.out.println("message"+message);
            return message;
        }
        return null;
    }

}


