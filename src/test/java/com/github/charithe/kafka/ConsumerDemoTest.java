
package com.github.charithe.kafka;

import com.example.Customer;
import com.github.charithe.kafka.deserialize.AvroResponseDeserializer;
import com.github.charithe.kafka.serialize.AvroToBytesSerializer;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ConsumerDemoTest {

    public static final String TEST_TOPIC = "test-topic";

    @Test
    public void testStartAndStop() throws Exception {
        int kafkaPort = InstanceSpec.getRandomPort();
        int zkPort = InstanceSpec.getRandomPort();
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(kafkaPort, zkPort);
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();
        assertThat(broker.getKafkaPort().get()).isEqualTo(kafkaPort);
        assertThat(broker.getZookeeperPort().get()).isEqualTo(zkPort);
        assertThat(broker.getBrokerList().isPresent()).isTrue();
        assertThat(broker.getZookeeperConnectString().isPresent()).isTrue();
        assertThat(broker.getLogDir().isPresent()).isTrue();

        Path logDir = Paths.get(broker.getLogDir().get());
        assertThat(Files.exists(logDir)).isTrue();

        broker.stop();
        assertThat(res.isDone()).isTrue();
        assertThat(broker.isRunning()).isFalse();
        assertThat(broker.getBrokerList().isPresent()).isFalse();
        assertThat(broker.getZookeeperConnectString().isPresent()).isFalse();
        assertThat(Files.exists(logDir)).isFalse();
    }


    @Test
    public void testConsumer() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 1281);
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();

        try (KafkaProducer<String, String> producer =
                     broker.createProducer(new StringSerializer(), new StringSerializer(), null)) {
            Future<RecordMetadata> result =
                    producer.send(new ProducerRecord<>(TEST_TOPIC, "key1", "value1"));

            RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo(TEST_TOPIC);
        }

        ConsumerDemo consumerDemo = new ConsumerDemo("group1");

        consumerDemo.subscribe(TEST_TOPIC);
        ConsumerRecords<String, String> records;
        records = consumerDemo.poll(10000);
        assertThat(records).isNotNull();
        assertThat(records.isEmpty()).isFalse();

        ConsumerRecord<String, String> msg = records.iterator().next();
        assertThat(msg).isNotNull();
        assertThat(msg.key()).isEqualTo("key1");
        assertThat(msg.value()).isEqualTo("value1");


        broker.stop();
    }

    @Test
    public void testProducerConsumer() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);
        ProducerDemo producerDemo = new ProducerDemo();
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();
        Future<RecordMetadata> result = producerDemo.send(TEST_TOPIC, "key1", "value1");
        RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(TEST_TOPIC);

        ConsumerDemo consumerDemo = new ConsumerDemo("group1");

        consumerDemo.subscribe(TEST_TOPIC);
        ConsumerRecords<String, String> records;
        records = consumerDemo.poll(10000);
        assertThat(records).isNotNull();
        assertThat(records.isEmpty()).isFalse();

        ConsumerRecord<String, String> msg = records.iterator().next();
        assertThat(msg).isNotNull();
        assertThat(msg.key()).isEqualTo("key1");
        assertThat(msg.value()).isEqualTo("value1");

        broker.stop();
    }
}
