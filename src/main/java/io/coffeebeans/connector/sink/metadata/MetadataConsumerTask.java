package io.coffeebeans.connector.sink.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.model.Metadata;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ConcurrentMap;

public class MetadataConsumerTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MetadataConsumerTask.class);

    private final ObjectMapper objectMapper;
    private final KafkaConsumer<Object, Object> kafkaConsumer;
    private final ConcurrentMap<String, Integer> currentActiveIndexMap;

    public MetadataConsumerTask(KafkaConsumer<Object, Object> kafkaConsumer,
                                ConcurrentMap<String, Integer> currentActiveIndexMap) {

        this.objectMapper = new ObjectMapper();
        this.kafkaConsumer = kafkaConsumer;
        this.currentActiveIndexMap = currentActiveIndexMap;
    }
    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        while (true) {
            ConsumerRecords<Object, Object> records = kafkaConsumer.poll(Duration.ofMinutes(1));

            records.forEach((record) -> {
                try {
                    Metadata metadata = objectMapper.readValue((byte[]) record.value(), Metadata.class);
                    currentActiveIndexMap.put(metadata.getFolderPath(), metadata.getIndex());

                    logger.info("{}: {}", metadata.getFolderPath(), metadata.getIndex());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
