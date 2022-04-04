package io.coffeebeans.connector.sink.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.coffeebeans.connector.sink.model.Metadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MetadataProducer {

    private ObjectMapper objectMapper;
    private String topicName = "metadata";
    private String bootstrapServers = "kafka:9092";
    private KafkaProducer<Object, Object> kafkaProducer;

    public MetadataProducer() {
        objectMapper = new ObjectMapper();

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    public void produceMetadata(Metadata metadata) throws JsonProcessingException {
        byte[] metadataBytes = objectMapper.writeValueAsBytes(metadata);

        ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(
                topicName, 0, "nokey", metadataBytes
        );

        this.kafkaProducer.send(record);
        this.kafkaProducer.flush();
    }
}
