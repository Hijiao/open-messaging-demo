package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.util.*;

public class DefaultPullConsumer implements PullConsumer {
    //    private MessageStore messageStore = MessageStore.getInstance();
    private MessageFileStore messageStore = MessageFileStore.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();

    private PageCacheReadUnitQueueManager queueManager = PageCacheReadUnitQueueManager.getInstance();
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        queueManager.setFilePath(properties.getString("STORE_PATH"));
    }


    @Override
    public KeyValue properties() {
        return properties;
    }


    @Override
    public Message poll() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }
        Message message = messageStore.pullMessage(false, queue);
        if (message != null) {
            return message;
        }

        for (int i = 0; i < bucketList.size(); i++) {
            message = messageStore.pullMessage(true, bucketList.get(i));
            if (message != null) {
                return message;
            }
        }
        return null;
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
        //buckets.add(queueName);
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
    }


}
