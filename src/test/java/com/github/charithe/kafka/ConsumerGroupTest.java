package com.github.charithe.kafka;

import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ConsumerGroupTest {

    public static final String TEST_TOPIC = "test-topic";
    public static int numberOfMessage = 10;

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
    public void testProducerConsumerDifferentGroupId() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);

        ProducerDemo producerDemo = new ProducerDemo();
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();
        ConsumerDemo consumerDemo = new ConsumerDemo("group1");
        ConsumerDemo consumerDemo1 = new ConsumerDemo("group2");
        Future<RecordMetadata> result = producerDemo.send(TEST_TOPIC, "key1", "value1");
        Future<RecordMetadata> result1 = producerDemo.send(TEST_TOPIC, "key2", "value2");
        RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(TEST_TOPIC);

        RecordMetadata metadata1 = result1.get(500L, TimeUnit.MILLISECONDS);
        assertThat(metadata1).isNotNull();
        assertThat(metadata1.topic()).isEqualTo(TEST_TOPIC);

        consumerDemo.subscribe(TEST_TOPIC);
        consumerDemo1.subscribe(TEST_TOPIC);
        ConsumerRecords<String, String> records;
        records = consumerDemo.poll(10000);
        assertThat(records).isNotNull();
        assertThat(records.isEmpty()).isFalse();
        ConsumerRecords<String, String> records1;
        records1 = consumerDemo1.poll(10000);
        assertThat(records1).isNotNull();
        assertThat(records1.isEmpty()).isFalse();

        ConsumerRecord<String, String> msg = records.iterator().next();
        assertThat(msg).isNotNull();
        assertThat(msg.key()).isEqualTo("key1");
        assertThat(msg.value()).isEqualTo("value1");

        ConsumerRecord<String, String> msg1 = records1.iterator().next();
        assertThat(msg1).isNotNull();
        assertThat(msg1.key()).isEqualTo("key1");
        assertThat(msg1.value()).isEqualTo("value1");

        broker.stop();
    }

    @Test
    public void testProducerConsumerSameGroupId() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);

        ProducerDemo producerDemo = new ProducerDemo();
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();

        ConsumerDemo consumerDemo = new ConsumerDemo("group1");
        ConsumerDemo consumerDemo1 = new ConsumerDemo("group1");

        consumerDemo.subscribe(TEST_TOPIC);
        consumerDemo1.subscribe(TEST_TOPIC);

        AtomicInteger atomicInteger = new AtomicInteger(0);
        Thread thread1 = new Thread(() -> {
            while (atomicInteger.get() !=numberOfMessage) {
                ConsumerRecords<String, String> records;
                records = consumerDemo.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(" thread1 Value: " + record.value());
                    System.out.println(" thread1 Partition: " + record.partition() + ", Offset:" + record.offset());
                    atomicInteger.incrementAndGet();
                }
            }
        });

        Thread thread2 = new Thread(() -> {
            while (atomicInteger.get() !=numberOfMessage) {
                ConsumerRecords<String, String> records;
                records = consumerDemo1.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("thread2 Value: " + record.value());
                    System.out.println("thread2 Partition: " + record.partition() + ", Offset:" + record.offset());
                    atomicInteger.incrementAndGet();
                }
            }
        });
        thread1.start();
        thread2.start();

        for (int i = 0; i < numberOfMessage; i++) {
            producerDemo.send(TEST_TOPIC,  "value" + i);
        }
        producerDemo.flushAndCloseProducer();
        thread1.join();
        thread2.join();
        broker.stop();
        Assert.assertEquals(atomicInteger.get(), numberOfMessage);
    }
}
