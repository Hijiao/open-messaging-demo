package io.openmessaging.demo;

import io.openmessaging.*;

public class DefaultProducer implements Producer {

    private MessageFactory messageFactory = new DefaultMessageFactory();
    //    private MessageStore messageStore =MessageStore.getInstance();
    private MessageFileStore messageStore = MessageFileStore.getInstance();

    private static volatile boolean fistInit = false;

    private KeyValue properties;

    public DefaultProducer() {
    }

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        synchronized (DefaultProducer.class) {
            if (!fistInit) {
                PageCacheManager.getInstance().setStorePath(properties.getString("STORE_PATH"));
                PageCacheManager.getInstance().start();
                fistInit = true;
            }
        }
    }


    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public void send(Message message) {
//        if (message.headers().keySet().size() != 1 & header_count < 100) {
//            ++header_count;
////            System.out.println("-------------------");
//
//
//            if (message.headers().getString(MessageHeader.TOPIC) != null) {
//                System.out.println(message.headers().getString(MessageHeader.TOPIC) + ":hea-> " + message.headers().toString());
//            } else {
//                System.out.println(message.headers().getString(MessageHeader.QUEUE) + ":hea->" + message.headers().toString());
//            }
//
//        }
//
//        if (message.properties() != null & pro_count < 100) {
//            ++pro_count;
////            System.out.println("-------------------");
//            if (message.headers().getString(MessageHeader.TOPIC) != null) {
//                System.out.println(message.headers().getString(MessageHeader.TOPIC) + ":pro-> " + message.properties().toString());
//            } else {
//                System.out.println(message.headers().getString(MessageHeader.QUEUE) + ":pro-> " + message.properties().toString());
//
//            }
//
//        }

        //if (message == null) throw new ClientOMSException("Message should not be null");
//        String topic = message.headers().getString(MessageHeader.TOPIC);
//        String queue = message.headers().getString(MessageHeader.QUEUE);
//        if ((topic == null && queue == null) || (topic != null && queue != null)) {
//            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
//        }
//        if (topic != null) {
//            messageStore.putMessage(true, topic, message);
//
//        } else {
//            messageStore.putMessage(false, queue, message);
//        }
        synchronized (DefaultProducer.class) {
            messageStore.putMessage(message);
        }


    }

    @Override
    public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void flush() {
//        BlockingQueue<Runnable> queue=executor.getThreads();
//
//        for (Runnable r :queue){
//            (PageCacheWriteUnitQueueManager)r.
//        }
        messageStore.flushWriteBuffers();
        PageCacheManager.getInstance().flushOffsetToFile();
//        //手动刷掉缓存
//        String[] commands = {"/bin/sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"};
//        try {
//            Process pr = Runtime.getRuntime().exec(commands);
//            pr.waitFor();
//            System.out.println("flushed  cache!");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
