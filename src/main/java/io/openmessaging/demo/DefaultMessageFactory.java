package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;

public class DefaultMessageFactory implements MessageFactory {

    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
        return defaultBytesMessage;
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.QUEUE, queue);
        return defaultBytesMessage;
    }

    public static DefaultBytesMessage createByteMessage(String bucket, boolean isTopic) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage();
        if (isTopic) {
            defaultBytesMessage.putHeaders(MessageHeader.TOPIC, bucket);
        } else {
            defaultBytesMessage.putHeaders(MessageHeader.QUEUE, bucket);
        }
        return defaultBytesMessage;

    }

}
