package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class DefaultPullConsumer implements PullConsumer {
    private MessageFileStore messageStore = MessageFileStore.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> topics = new HashSet<>();

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        PageCacheReadUnitQueueManager.getInstance().setFilePath(properties.getString("STORE_PATH"));
    }


    @Override
    public KeyValue properties() {
        return properties;
    }


    int count = 0;
    int tc = 0;

    private void showQMessage(Message message) {
        if (count++ % 10000000 == 0) {
            System.out.println(((DefaultBytesMessage) message).toString());
        }
    }

    private void showTMessage(Message message) {
        if (tc++ % 10000000 == 0) {
            System.out.println(((DefaultBytesMessage) message).toString());
        }
    }

    @Override
    public Message poll() {


//        if (topics.size() == 0 || queue == null) {
//            return null;
//        }
        Message message = messageStore.pullMessage(queue);
        if (message != null) {
            showQMessage(message);
            return message;
        }
        for (String topic : topics) {
            message = messageStore.pullMessage(topic);
            if (message != null) {
                showTMessage(message);
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
        //topics.add(queueName);
        this.topics.addAll(topics);

        PageCacheReadUnitQueueManager.intPageCacheReadUnitQueu(queueName, false);

        for (String topic : topics) {
            PageCacheReadUnitQueueManager.intPageCacheReadUnitQueu(topic, true);
        }


    }


}
