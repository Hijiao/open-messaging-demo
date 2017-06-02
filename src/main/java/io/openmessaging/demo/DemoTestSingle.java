package io.openmessaging.demo;

import io.openmessaging.*;

import java.util.Collections;

/**
 * Created by Max on 2017/5/30.
 */
public class DemoTestSingle {
    public static void main(String[] args) {
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/Users/Max/code/tianchi/tmp");
        Producer producer = new DefaultProducer(properties);

        String topic1 = "TOPIC1"; //实际测试时大概会有100个Topic左右

        BytesMessage b = producer.createBytesMessageToTopic(topic1, (topic1 + 1).getBytes());
        b.putHeaders("MessageId", "cccc");
        b.putProperties("PRO_OFFSET", "PRODUCER1_1");
        b.putProperties("dprq1", "s465d");
        producer.send(b);
        producer.flush();

        PullConsumer consumer = new DefaultPullConsumer(properties);
        consumer.attachQueue("qq", Collections.singletonList(topic1));

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Message result = consumer.poll();
        System.out.println("result: " + result);

        String topicName = result.headers().getString(MessageHeader.TOPIC);
        System.out.println("topicName:" + topicName);


    }

}
