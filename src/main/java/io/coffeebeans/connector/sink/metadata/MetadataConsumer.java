package io.coffeebeans.connector.sink.metadata;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

public class MetadataConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MetadataConsumer.class);
    private final String topicName = "metadata";
    private final String bootstrapServers = "kafka:9092";

    private ConcurrentMap<String, Integer> currentActiveIndexMap;

    public MetadataConsumer(ConcurrentMap<String, Integer> currentActiveIndexMap) {
        this.currentActiveIndexMap = currentActiveIndexMap;
    }

    public void pollMetadata() {
        KafkaConsumer<Object, Object> kafkaConsumer = createKafkaConsumer();
        kafkaConsumer.subscribe(List.of(topicName));
        MetadataConsumerTask task = new MetadataConsumerTask(kafkaConsumer, currentActiveIndexMap);

        Thread metadataConsumerThread = new Thread(task);
        try {
            metadataConsumerThread.start();
        } catch (Exception e) {
            pollMetadata();
        }
    }

    private KafkaConsumer<Object, Object> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, String.valueOf(new Random().nextInt(1000)));

        return new KafkaConsumer<>(properties);
    }
}
