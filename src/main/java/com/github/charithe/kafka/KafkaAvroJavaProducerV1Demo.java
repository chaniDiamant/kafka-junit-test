package com.github.charithe.kafka;

import com.example.Customer;
import com.github.charithe.kafka.serialize.AvroToBytesSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaAvroJavaProducerV1Demo {

    Producer<String, Customer> producer;

    public  KafkaAvroJavaProducerV1Demo() {
        // create the producer
        producer = new KafkaProducer<>(getProperties());
    }

    public Properties getProperties() {

        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", AvroToBytesSerializer.class.getName());
        //properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        return properties;
    }

    public Future<RecordMetadata> send(String topic, String key, Customer customer) {
        ProducerRecord<String, Customer> record =
                new ProducerRecord<String, Customer>(topic,key, customer);
        // send data - asynchronous
        Future<RecordMetadata> metadata=producer.send(record);
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
        return metadata;
    }
}
