package io.coffeebeans.connector.sink.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.model.Metadata;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This task launched by MetadataConsumer will run on a separate thread and will poll metadata from kafka topic.
 */
public class MetadataConsumerTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MetadataConsumerTask.class);

    private final ObjectMapper objectMapper;
    private final KafkaConsumer<Object, Object> kafkaConsumer;
    private final ConcurrentMap<String, Integer> currentActiveIndexMap;

    /**
     * Constructor with kafka consumer instance and the active index map as parameters.
     *
     * @param kafkaConsumer Kafka consumer
     * @param currentActiveIndexMap The map containing the active index which needs to be updated
     */
    public MetadataConsumerTask(KafkaConsumer<Object, Object> kafkaConsumer,
                                ConcurrentMap<String, Integer> currentActiveIndexMap) {

        this.objectMapper = new ObjectMapper();
        this.kafkaConsumer = kafkaConsumer;
        this.currentActiveIndexMap = currentActiveIndexMap;
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<Object, Object> records = kafkaConsumer.poll(Duration.ofMinutes(1));

            records.forEach((record) -> {
                try {
                    Metadata metadata = objectMapper.readValue((byte[]) record.value(), Metadata.class);
                    currentActiveIndexMap.put(metadata.getFullPath(), metadata.getIndex());

                    logger.info("{}: {}", metadata.getFullPath(), metadata.getIndex());
                } catch (IOException e) {
                    logger.error("Error while parsing the received metadata with exception: {}", e.getMessage());
                }
            });
        }
    }
}
