package io.openmessaging.demo;

import io.openmessaging.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;

public class DemoTester {
    private static final int MESSAGE_LENGTH = 300;

    public static void main(String[] args) {
        KeyValue properties = new DefaultKeyValue();
        /*
        //实际测试时利用 STORE_PATH 传入存储路径
        //所有producer和consumer的STORE_PATH都是一样的，选手可以自由在该路径下创建文件
         */
        //properties.put("STORE_PATH", "/home/admin/test");
        properties.put("STORE_PATH", "/Users/Max/code/tianchi/tmp");

        //这个测试程序的测试逻辑与实际评测相似，但注意这里是单线程的，实际测试时会是多线程的，并且发送完之后会Kill进程，再起消费逻辑

        Producer producer = new DefaultProducer(properties);

        //构造测试数据
        String topic1 = "TOPIC1"; //实际测试时大概会有100个Topic左右
        String topic2 = "TOPIC2"; //实际测试时大概会有100个Topic左右
        String queue1 = "QUEUE1"; //实际测试时，queue数目与消费线程数目相同
        String queue2 = "QUEUE2"; //实际测试时，queue数目与消费线程数目相同
        List<Message> messagesForTopic1 = new ArrayList<>(MESSAGE_LENGTH);
        List<Message> messagesForTopic2 = new ArrayList<>(MESSAGE_LENGTH);
        List<Message> messagesForQueue1 = new ArrayList<>(MESSAGE_LENGTH);
        List<Message> messagesForQueue2 = new ArrayList<>(MESSAGE_LENGTH);
        for (int i = 0; i < MESSAGE_LENGTH; i++) {
            //注意实际比赛可能还会向消息的headers或者properties里面填充其它内容
            BytesMessage b = producer.createBytesMessageToTopic(topic1, (topic1 + i).getBytes());
            b.putHeaders("test_header_key", "test_header_value");
            b.putProperties("test_pro_key", "test_pro_value");
            b.putProperties("test_pro_key2", "test_pro_value2");

            messagesForTopic1.add(b);
            messagesForTopic2.add(producer.createBytesMessageToTopic(topic2,  (topic2 + i).getBytes()));
            messagesForQueue1.add(producer.createBytesMessageToQueue(queue1, (queue1 + i).getBytes()));
            messagesForQueue2.add(producer.createBytesMessageToQueue(queue2, (queue2 + i).getBytes()));
        }

        long start = System.currentTimeMillis();
        //发送, 实际测试时，会用多线程来发送, 每个线程发送自己的Topic和Queue
        for (int i = 0; i < MESSAGE_LENGTH; i++) {
            producer.send(messagesForTopic1.get(i));
            producer.send(messagesForTopic2.get(i));
            producer.send(messagesForQueue1.get(i));
            producer.send(messagesForQueue2.get(i));
        }
        long end = System.currentTimeMillis();

        long T1 = end - start;
        //show all data in file store
        MessageFileStore fileStore = MessageFileStore.getInstance();
        //fileStore.showAllBuckets();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //请保证数据写入磁盘中

        //消费样例1，实际测试时会Kill掉发送进程，另取进程进行消费
        {
            PullConsumer consumer1 = new DefaultPullConsumer(properties);
            consumer1.attachQueue(queue1, Collections.singletonList(topic1));

            int queue1Offset = 0, topic1Offset = 0;

            long startConsumer = System.currentTimeMillis();
            while (true) {
                Message message = consumer1.poll();
                if (message == null) {
                    //System.out.println("null consume message");
                    //拉取为null则认为消息已经拉取完毕
                    break;
                }
                // System.out.println("consume -->" + new String(((BytesMessage) message).getBody()));
                String topic = message.headers().getString(MessageHeader.TOPIC);
                String queue = message.headers().getString(MessageHeader.QUEUE);
                //实际测试时，会一一比较各个字段
                if (topic != null) {
                    //Assert.assertEquals(topic1, topic);

                    System.out.println(new String(((BytesMessage) messagesForTopic1.get(topic1Offset++)).getBody()) + "===" + new String(((BytesMessage) message).getBody()));
                    //Assert.assertEquals(messagesForTopic1.get(topic1Offset++), message);
                } else {
                    Assert.assertEquals(queue1, queue);
                    //Assert.assertEquals(messagesForQueue1.get(queue1Offset++), message);
                    System.out.println(new String(((BytesMessage) messagesForQueue1.get(queue1Offset++)).getBody()) + "===" + new String(((BytesMessage) message).getBody()));

                }
            }
            long endConsumer = System.currentTimeMillis();
            long T2 = endConsumer - startConsumer;
            System.out.println(String.format("Team1 cost:%d ms tps:%d q/ms", T2 + T1, (queue1Offset + topic1Offset)/(T1 + T2)));

        }

        //消费样例2，实际测试时会Kill掉发送进程，另取进程进行消费
        {
            PullConsumer consumer2 = new DefaultPullConsumer(properties);
            List<String> topics = new ArrayList<>();
            topics.add(topic1);
            topics.add(topic2);
            consumer2.attachQueue(queue2, topics);

            int queue2Offset = 0, topic1Offset = 0, topic2Offset = 0;

            long startConsumer = System.currentTimeMillis();
            while (true) {
                Message message = consumer2.poll();
                if (message == null) {
                    //拉取为null则认为消息已经拉取完毕
                    break;
                }
                //System.out.println("consume -->" + new String(((BytesMessage) message).getBody()));
                String topic = message.headers().getString(MessageHeader.TOPIC);
                String queue = message.headers().getString(MessageHeader.QUEUE);
                //实际测试时，会一一比较各个字段
                if (topic != null) {
                    if (topic.equals(topic1)) {
                        System.out.println(new String(((BytesMessage) messagesForTopic1.get(topic1Offset++)).getBody()) + "===" + new String(((BytesMessage) message).getBody()));

                        //Assert.assertEquals(messagesForTopic1.get(topic1Offset++), message);
                    } else {
                        //  Assert.assertEquals(topic2, topic);
                        // Assert.assertEquals(messagesForTopic2.get(topic2Offset++), message);
                    }
                } else {
                    Assert.assertEquals(queue2, queue);
                    System.out.println(new String(((BytesMessage) messagesForQueue2.get(queue2Offset++)).getBody()) + "===" + new String(((BytesMessage) message).getBody()));

                    // Assert.assertEquals(messagesForQueue2.get(queue2Offset++), message);
                }
            }
            long endConsumer = System.currentTimeMillis();
            long T2 = endConsumer - startConsumer;
            System.out.println(String.format("Team2 cost:%d ms tps:%d q/ms", T2 + T1, (queue2Offset + topic1Offset)/(T1 + T2)));
        }


    }
}
