package io.coffeebeans.connector.sink.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connector.sink.model.Metadata;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * This class is responsible for producing metadata to the kafka metadata topic.
 */
public class MetadataProducer {

    private final ObjectMapper objectMapper;
    private final KafkaProducer<Object, Object> kafkaProducer;

    /**
     * Constructor with Config class parameter.
     *
     * @param config AzureBlobSinkConfig
     */
    public MetadataProducer(AzureBlobSinkConfig config) {
        objectMapper = new ObjectMapper();
        String bootstrapServers = config.getMetadataBootstrapServers();

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    /**
     * I will produce the metadata to the kafka topic.
     *
     * @param metadata Metadata to be produced
     * @throws JsonProcessingException - Thrown if exception occur during converting metadata to byte array
     */
    public void produceMetadata(Metadata metadata) throws JsonProcessingException {
        byte[] metadataBytes = objectMapper.writeValueAsBytes(metadata);

        String topicName = "metadata";
        ProducerRecord<Object, Object> record = new ProducerRecord<>(
                topicName, 0, "nokey", metadataBytes
        );

        this.kafkaProducer.send(record);
        this.kafkaProducer.flush();
    }
}
