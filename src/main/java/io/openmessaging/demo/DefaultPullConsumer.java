package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultPullConsumer implements PullConsumer {
    private MessageFileStore messageStore = MessageFileStore.getInstance();
    private KeyValue properties;
    private String queue;
    private List<String> buckets = new ArrayList<>();

    private static volatile boolean fistInit = false;


    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        synchronized (DefaultPullConsumer.class) {
            if (!fistInit) {
                init(properties.getString("STORE_PATH"));
                fistInit = true;
            }
        }
    }

    private static final Map<String, AtomicInteger> counter = new HashMap<String, AtomicInteger>();

    private static final Map<String, Integer> maxOffset = new HashMap<>();


    private static void init(String storePath) {
        PageCacheReaderManager.getInstance().setStorePath(storePath);
        PageCacheReaderManager.getInstance().start();


        try {

            File file = new File(storePath);
            FileInputStream in = new FileInputStream(file);
            int size = in.available();

            byte[] buffer = new byte[size];

            in.read(buffer);

            in.close();
            String s = new String(buffer);
            String[] records = s.split(",");

            for (String record : records) {
                System.out.println(record.split("=")[0]);
                System.out.println(record.split("=")[1]);
                PageCacheReadUnitQueueManager.intPageCacheReadUnitQueue(record.split("=")[0]);
                maxOffset.put(record.split("=")[0], Integer.parseInt(record.split("=")[1]));
                counter.put(record.split("=")[0], new AtomicInteger());
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public KeyValue properties() {
        return properties;
    }


//    int count = 0;
//    int tc = 0;
//
//    private void showQMessage(Message message) {
//        if (count++ % 10000000 == 0) {
//            System.out.println(((DefaultBytesMessage) message).toString());
//        }
//    }
//
//    private void showTMessage(Message message) {
//        if (tc++ % 10000000 == 0) {
//            System.out.println(((DefaultBytesMessage) message).toString());
//        }
//    }

    @Override
    public Message poll() {

        for (String bucket : buckets) {
            int offset = counter.get(bucket).decrementAndGet();
            if (offset > maxOffset.get(bucket)) {
                return null;
            }

            Message message = messageStore.pullMessage()

        }

        return messageStore.pullMessage(buckets);

    }

    @Override
    public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.add(queueName);
        buckets.addAll(topics);

        for (String bucket : buckets) {
            PageCacheReadUnitQueueManager.intPageCacheReadUnitQueue(bucket);
        }


    }


}
