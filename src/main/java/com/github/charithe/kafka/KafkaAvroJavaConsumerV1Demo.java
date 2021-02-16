package com.github.charithe.kafka;

import com.example.Customer;
import com.github.charithe.kafka.deserialize.AvroResponseDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroJavaConsumerV1Demo {

    KafkaConsumer<String, Customer> consumer;

    public KafkaAvroJavaConsumerV1Demo() {
        // create consumer
        consumer = new KafkaConsumer<>(getProperties());
    }

    public Properties getProperties() {
        Properties properties = new Properties();
        // normal consumer
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.put("group.id", "customer-consumer-group-v1");
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "earliest");

        // avro part (deserializer)
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", AvroResponseDeserializer.class.getName());
        properties.setProperty("specific.avro.reader", "true");
        return properties;
    }

    public void subscribe(String topic) {
        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
    }

    public ConsumerRecords<String, Customer> poll(int duration){
        ConsumerRecords<String, Customer> records =
                consumer.poll(Duration.ofMillis(duration)); // new in Kafka 2.0.0
        return records;
    }
}
