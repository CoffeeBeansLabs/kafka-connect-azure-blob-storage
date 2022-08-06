package io.coffeebeans.connector.sink.format.avro;

import io.coffeebeans.connector.sink.format.AzureBlobOutputStream;
import io.coffeebeans.connector.sink.format.RecordWriter;
import io.coffeebeans.connector.sink.format.SchemaStore;
import io.coffeebeans.connector.sink.storage.StorageManager;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import java.io.IOException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroRecordWriter implements RecordWriter {
    private final Logger log = LoggerFactory.getLogger(AvroRecordWriter.class);
    private static final int AVRO_DATA_CACHE_SIZE = 20;

    private Schema kafkaValueSchema;
    private AzureBlobOutputStream outputStream;
    private final DataFileWriter<Object> dataFileWriter;

    private final int partSize;
    private final String blobName;
    private final AvroData avroData;
    private final String kafkaTopic;
    private final SchemaStore schemaStore;
    private final StorageManager storageManager;

    public AvroRecordWriter(StorageManager storageManager,
                            SchemaStore schemaStore,
                            int partSize,
                            String blobName,
                            String kafkaTopic) {

        this.partSize = partSize;
        this.blobName = blobName;
        this.kafkaTopic = kafkaTopic;
        this.schemaStore = schemaStore;
        this.storageManager = storageManager;
        this.avroData = new AvroData(AVRO_DATA_CACHE_SIZE);

        this.dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>());
    }

    @Override
    public void write(SinkRecord kafkaRecord) throws IOException {
        if (kafkaValueSchema == null) {
            log.info("Opening Avro record writer for blob: {}", blobName);

            kafkaValueSchema = kafkaRecord.valueSchema();
            org.apache.avro.Schema avroValueSchema = avroData
                    .fromConnectSchema(kafkaValueSchema);

            outputStream = new AzureBlobOutputStream(
                    storageManager,
                    blobName,
                    partSize
            );
            dataFileWriter.create(avroValueSchema, outputStream);
        }
        Object value = avroData
                .fromConnectData(kafkaValueSchema, kafkaRecord.value());

        if (value instanceof NonRecordContainer) {
            value = ((NonRecordContainer) value).getValue();
        }
        dataFileWriter.append(value);
    }

    @Override
    public void close() throws IOException {
        dataFileWriter.close();
    }

    @Override
    public void commit() throws IOException {
        dataFileWriter.flush();
        outputStream.commit();
        dataFileWriter.close();
    }

    @Override
    public long getDataSize() {
        return 0L;
    }
}
