package com.github.charithe.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    KafkaConsumer<String, String> consumer;

    public ConsumerDemo(String groupId) {
        // create consumer
        consumer = new KafkaConsumer<>(getProperties(groupId));
    }

    public Properties getProperties(String groupId) {
        String bootstrapServers = "127.0.0.1:9092";
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }

    public void subscribe(String topic) {
        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
    }

    public ConsumerRecords<String, String> poll(int duration){
        ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(duration)); // new in Kafka 2.0.0
        return records;
    }
}


