package io.coffeebeans.connector.sink.metadata;

import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * I consume the metadata produced by the other kafka topics and update the local metadata.
 */
public class MetadataConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MetadataConsumer.class);
    private final String bootstrapServers;
    private final ConcurrentMap<String, Integer> currentActiveIndexMap;

    public MetadataConsumer(AzureBlobSinkConfig config, ConcurrentMap<String, Integer> currentActiveIndexMap) {
        this.currentActiveIndexMap = currentActiveIndexMap;
        this.bootstrapServers = config.getMetadataBootstrapServers();
    }

    /**
     * Start polling metadata from kafka topic.
     */
    public void startPolling() {
        KafkaConsumer<Object, Object> kafkaConsumer = createKafkaConsumer();
        String topicName = "metadata";
        kafkaConsumer.subscribe(List.of(topicName));
        MetadataConsumerTask task = new MetadataConsumerTask(kafkaConsumer, currentActiveIndexMap);

        Thread metadataConsumerThread = new Thread(task);
        try {
            logger.info("Starting metadata polling");
            metadataConsumerThread.start();
        } catch (Exception e) {
            logger.error("Consumer interrupted with exception: {}", e.getMessage());
            logger.error("Trying to restart polling ....");
            startPolling();
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
