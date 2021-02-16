package com.github.charithe.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerDemo {


    KafkaProducer<String, String> producer;

    public  ProducerDemo() {
        // create the producer
        producer = new KafkaProducer<>(getProperties());
    }

    public Properties getProperties() {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1" + ":" + 9092);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }

    public Future<RecordMetadata> send(String topic, String key, String value) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic,key, value);
        // send data - asynchronous
        Future<RecordMetadata> metadata=producer.send(record);
        return metadata;
    }

    public Future<RecordMetadata> send(String topic,  String value) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, value);
        // send data - asynchronous
        Future<RecordMetadata> metadata=producer.send(record);
        return metadata;
    }

    public void flushAndCloseProducer() {
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
