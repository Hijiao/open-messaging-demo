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
        b.putHeaders("MessageId", "cgz8u4cwbcz8gzxjpfg27anfsoqvqxuokghyeq6tj0citklefmtz4sj0p5k1p78pwf4fpty8zxckb628n833tw1ovaabx7dy7hg89vkivac4rl2yt7fx9agvh52i1o1wbj8nejpxcwtub9enlf8fuh4nadxzz0ux21m298nefhe8u4sxjxxvlplgrpam3wws7680ttu8x5rey3lwicpdmrazy8ebhdfq1rgqutrsto6bctc814ts8rqrgd7a6qp9p9fp2ww17edkjlj0eabtiv2nvmhlswweihueubowwqf1hzxr2h4s0ahhzpund09hy8qgv9trde9ov2zownb80zw7uf4g5zq2plcb6apgqk366ft4ibv81dlrermdszurpprczeowoky2u2w3k4wu7zac2w13jz2r5w8tqtf2lladse8hfulmy4lb5ssosjgszrxurb3uhx1h9z4gphodh6ehk1n5pesufklfdtlkk5hq25q365lbyfmszvh9sxfs4j4upyzjm86upqo3js1arvc0m3mgtpbromjj9aphoim6j87aqctdfz2pmpihjslg4d7j76lu0ura8l5smowited4mlj60ygthtml9fbedde6aaawy0g0kk04g2ijowjskhlarrf8o2rpcmyplo1b9v9u3cpihr8kmv2bswnwtkccm6jxtivqrxed6bgdpt03ucztqhxwwf6tx0holvbuyr37l2k6lsbrs5qpp9o7081cnflcr92s203rx3urmpgo37rk7bmz890a3alz08orah2zd3iwef12cx7dbyfhspcaiv29sqyft8ywk8g71zu9ky7wqg3lf7fyvdtx38zicufnkwrgdrreislathdbffcobao7b6q1djbwhqhhjyenmzbqec726sqed3l0eyhbpmqi9k3o1x0556u9l424pmspapxqmtacxzrneoqv4k6m3zjptpfquh8zwd0x7jvhvhbsaj7szv1dy6q29gewc");
        b.putProperties("PRO_OFFSET", "PRODUCER1_1");
        b.putProperties("dprq1", "s465d");
        producer.send(b);

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
