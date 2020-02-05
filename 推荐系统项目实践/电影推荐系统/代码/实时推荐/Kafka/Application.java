package cn.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {

    public static void main(String[] args) {
        String broker = "master:9092";
        String zookeeper = "master:2181";

        // 输入和输出的topic
        String fromTopic = "log";
        String toTopic = "recommender";

        // 定义kafka streaming的配置
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        config.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper);

        // 创建 kafka stream 配置对象
        StreamsConfig streamsConfig = new StreamsConfig(config);

        // 创建一个拓扑建构器
        TopologyBuilder builder = new TopologyBuilder();

        // 定义流处理的拓扑结构
        builder.addSource("SOURCE", fromTopic)
                .addProcessor("PROCESSOR", () -> new LogProcessor(), "SOURCE")
                .addSink("SINK", toTopic, "PROCESSOR");

        KafkaStreams streams = new KafkaStreams(builder, streamsConfig);

        streams.start();

        System.out.println("Kafka stream started...");

    }

}
