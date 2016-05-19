package com.zhou.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * kafka消息消费者
 * 在C:\Windows\System32\drivers\etc\hosts
 	加入zookeeper.connect配置的机器ip即可！
 * @author zhoudong
 *
 */
public class ConsumerSample {
	private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;

    public ConsumerSample(String zookeeper, String groupId, String topic) {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper,groupId));
        this.topic = topic;
    }

    //关闭consumer
    public void shutdown() {
        if (consumer != null)
            consumer.shutdown();
        if (executor != null)
            executor.shutdown(); //禁止在这个Executor中添加新的任务  
    }

    public void execute(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));// 一次从主题中获取数据的个数
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        
        //创建线程池
        executor = Executors.newFixedThreadPool(numThreads);

        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new ConsumerMsgTask(stream, threadNumber));
            threadNumber++;
        }
    }
    
    //配置
    private static ConsumerConfig createConsumerConfig(String a_zookeeper,
            String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public static void main(String[] arg) {
        String[] args = { "192.168.35.89:2181", "group-1", "zhoudong_test_topic", "10" };
        String zooKeeper = args[0];
        String groupId = args[1];
        String topic = args[2];
        int threads = Integer.parseInt(args[3]);

        ConsumerSample demo = new ConsumerSample(zooKeeper, groupId, topic);
        demo.execute(threads);
        
        //睡10秒等上面的都处理完成以后在关闭线程
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
        demo.shutdown();
    }

}
