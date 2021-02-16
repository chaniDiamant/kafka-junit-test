package com.github.charithe.kafka;

import com.example.Customer;
import com.github.charithe.kafka.deserialize.AvroResponseDeserializer;
import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ProducerAvroSchemaTest {

    public static final String TEST_TOPIC = "test-topic";
    private void initKafka() {
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        Properties defaultConfig = new Properties();
        defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
    }
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
    public void testProducerWithAvro() throws Exception {
        initKafka();
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);
        KafkaAvroJavaProducerV1Demo avroProducer = new KafkaAvroJavaProducerV1Demo();
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();
        Customer customer = Customer.newBuilder()
                .setAge(34)
                .setAutomatedEmail(false)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(178f)
                .setWeight(75f)
                .build();

        Future<RecordMetadata> result = avroProducer.send(TEST_TOPIC, "key1", customer);
        RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(TEST_TOPIC);

        try (KafkaConsumer<String, Customer> consumer =
                     broker.createConsumer(new StringDeserializer(), new AvroResponseDeserializer(), null)) {

            consumer.subscribe(Lists.newArrayList(TEST_TOPIC));
            ConsumerRecords<String, Customer> records;
            records = consumer.poll(10000);
            assertThat(records).isNotNull();
            assertThat(records.isEmpty()).isFalse();

            ConsumerRecord<String, Customer> msg = records.iterator().next();
            assertThat(msg).isNotNull();
            assertThat(msg.key()).isEqualTo("key1");
            assertThat(msg.value().get("first_name").toString().equals("John"));
            assertThat(msg.value().get("last_name").toString().equals("Doe"));
        }

        broker.stop();
    }

    @Test
    public void testProducerConsumerWithAvro() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);
        KafkaAvroJavaProducerV1Demo avroProducer = new KafkaAvroJavaProducerV1Demo();
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();
        Customer customer = Customer.newBuilder()
                .setAge(34)
                .setAutomatedEmail(false)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(178f)
                .setWeight(75f)
                .build();

        Future<RecordMetadata> result = avroProducer.send(TEST_TOPIC, "key1", customer);
        RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(TEST_TOPIC);

        KafkaAvroJavaConsumerV1Demo consumerDemo = new KafkaAvroJavaConsumerV1Demo();

        consumerDemo.subscribe(TEST_TOPIC);
        ConsumerRecords<String, Customer> records;
        records = consumerDemo.poll(10000);
        assertThat(records).isNotNull();
        assertThat(records.isEmpty()).isFalse();

        ConsumerRecord<String, Customer> msg = records.iterator().next();
        assertThat(msg).isNotNull();
        assertThat(msg.key()).isEqualTo("key1");
        assertThat(msg.value().get("first_name").toString().equals("John"));
        assertThat(msg.value().get("last_name").toString().equals("Doe"));


        broker.stop();
    }
}
