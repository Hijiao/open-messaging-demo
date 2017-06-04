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
    private List<String> topics = new ArrayList<>();

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

    int queueMaxOffset = 0;

    private static void init(String storePath) {
        try {

            File file = new File(storePath + File.separator + "offset.txt");
            FileInputStream in = new FileInputStream(file);
            int size = in.available();

            byte[] buffer = new byte[size];

            in.read(buffer);

            in.close();
            String s = new String(buffer);
            String[] records = s.split(",");

            for (String record : records) {
                PageCacheReadUnitQueueManager.intPageCacheReadUnitQueue(record.split("=")[0]);
                maxOffset.put(record.split("=")[0], Integer.parseInt(record.split("=")[1]));
                counter.put(record.split("=")[0], new AtomicInteger());
            }


            PageCacheReaderManager.getInstance().setStorePath(storePath);
            PageCacheReaderManager.getInstance().start();


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
        DefaultBytesMessage message;
        synchronized (DefaultPullConsumer.class) {
            Iterator<String> iterator = topics.iterator();
            while (iterator.hasNext()) {
                String topic = iterator.next();
//                System.out.println(topic + " . current queueOffset:" + counter.get(topic).get() + ",maxOffset:" + maxOffset.get(topic));
                PageCacheReadUnitQueue readUnitQueue = PageCacheReadUnitQueueManager.getBucketReadUnitQueue(topic);
                message = readUnitQueue.poll();
                if (message != null) {
//                    System.out.println(topic + " current queueOffset:" + counter.get(topic).get() + ",maxOffset:" + maxOffset.get(topic) + ", " + message);
                    if (counter.get(topic).incrementAndGet() >= maxOffset.get(topic)) {
                        System.out.println(topic + "remove topic. current queueOffset:" + counter.get(topic).get() + ",maxOffset:" + maxOffset.get(topic));
                        iterator.remove();
                    }
                    return message;
                }
            }

//            System.out.println(queue + " current queueOffset:" + counter.get(queue).get() + ",maxOffset:" + maxOffset.get(queue));


            if (counter.get(queue).incrementAndGet() < queueMaxOffset) {
                //  System.out.println("take form queue");
                return PageCacheReadUnitQueueManager.getBucketReadUnitQueue(queue).take();
            } else if (counter.get(queue).get() == queueMaxOffset) {
                // System.out.println("倒数第二个" + counter.get(queue).get() + "，max:" + queueMaxOffset);
                try {
                    // System.out.println("sleep 100ms");
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return PageCacheReadUnitQueueManager.getBucketReadUnitQueue(queue).take();
            }
//
//            int count = 0;
//            for (Integer i : maxOffset.values()) {
//                count += i;
//            }
//            System.out.println(count);
            return PageCacheReadUnitQueueManager.getBucketReadUnitQueue(queue).poll();
        }
        //  System.out.println("count" + count);
//            for (PageCacheReadUnitQueue q : PageCacheReadUnitQueueManager.getBucketReadUnitQueue().values()) {
//                if (!q.isEmpty()) {
//                    System.out.println(counter.get(q.getBucketName()).get() + "" + q);
//                }
//            }

//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            if (topics.isEmpty()) {
//                return PageCacheReadUnitQueueManager.getBucketReadUnitQueue(queue).poll();
//            } else {
//                this.poll();
//            }
//        }
//        return null;
    }

    private static int readCount = 0;

//    @Override
//    public Message poll() {
//        DefaultBytesMessage message;
//        synchronized (DefaultPullConsumer.class) {
//            while (!buckets.isEmpty()) {
//                Iterator<String> iterator = buckets.iterator();
//                while (iterator.hasNext()) {
//                    String buckets = iterator.next();
//                    //  System.out.println(topic + " . current queueOffset:" + counter.get(topic).get() + ",maxOffset:" + maxOffset.get(topic));
//                    PageCacheReadUnitQueue readUnitQueue = PageCacheReadUnitQueueManager.getBucketReadUnitQueue(buckets);
//                    message = readUnitQueue.poll();
//                    if (message != null) {
//                        //readCount++;
//                        //   System.out.println(buckets + " current queueOffset:" + counter.get(buckets).get() + ",maxOffset:" + maxOffset.get(buckets));
//                        if (counter.get(buckets).incrementAndGet() >= maxOffset.get(buckets)) {
//                            //                           System.out.println(buckets + "remove topic. current queueOffset:" + counter.get(buckets).get() + ",maxOffset:" + maxOffset.get(buckets));
//                            iterator.remove();
//                        }
//                        return message;
//                    }
//                }
//                //return  PageCacheReadUnitQueueManager.getBucketReadUnitQueue(queue).take();
//            }

//        int count = 0;
//        for (Integer i : maxOffset.values()) {
//            count += i;
//        }
//        System.out.println("countAll:"+count);
//        System.out.println("readCount:"+readCount);
    //return null;

//        }
//    }

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

        this.topics.addAll(topics);
        buckets.addAll(topics);
        Collections.sort(buckets);
        buckets.add(queueName);

        for (String bucket : buckets) {
            PageCacheReadUnitQueueManager.intPageCacheReadUnitQueue(bucket);
        }
        queueMaxOffset = maxOffset.get(queue) - 1;


    }


}
