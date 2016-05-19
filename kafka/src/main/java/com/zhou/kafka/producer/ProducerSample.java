package com.zhou.kafka.producer;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * kafka消息生产者
 * @author zhoudong
 *
 */
public class ProducerSample {

	public static void main(String[] args) {
        // 设置配置属性
        Properties props = new Properties();
        props.put("metadata.broker.list","192.168.35.89:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // key.serializer.class默认为serializer.class
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
        // 值为0,1,-1,可以参考
        // http://kafka.apache.org/08/configuration.html
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        // 创建producer
        Producer<String, String> producer = new Producer<String, String>(config);
        // 产生并发送消息
        long start = System.currentTimeMillis();
        for (long i = 0; i < 30; i++) {
            long runtime = new Date().getTime();
            
            String  key = "192.168.35." + i;//key 可以可以不传
            String msg = runtime + ",这是一条消息," + key;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(
                    "zhoudong_test_topic", key, msg); //zhoudong_test_topic topic 如果不存在会自动创建
            
            /*String msg = runtime + ",这是一条消息！！";
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("zhoudong_test_topic", msg);*/
            producer.send(data);
        }
        System.out.println("耗时:" + (System.currentTimeMillis() - start));
        // 关闭producer
        producer.close();
	}

}
