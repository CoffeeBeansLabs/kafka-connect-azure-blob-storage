package io.coffeebeans.connector.sink.format.parquet;

import io.coffeebeans.connector.sink.format.FormatWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * I will write the records in parquet file format.
 */
public class ParquetFormatWriter implements FormatWriter {
    private static final Logger logger = LoggerFactory.getLogger(FormatWriter.class);

    private int recordsWritten;
    private final Schema avroSchema;
    private ParquetOutputFile parquetOutputFile;
    private ParquetWriter<GenericData.Record> parquetWriter;

    /**
     * Constructor with Avro schema as parameter.
     *
     * @param avroSchema Avro schema of the records that will be written
     * @throws IOException - Thrown if exception occur during writing records by the parquet writer
     */
    public ParquetFormatWriter(Schema avroSchema) throws IOException {
        this.avroSchema = avroSchema;
        instantiateNewParquetWriter(avroSchema);
    }

    /**
     * I store the writer, write using that and maintain the number of records written by the writer.
     *
     * @param data Value map
     * @throws IOException thrown if exception occur during writing the data by the writer
     */
    public void write(Map<String, Object> data) throws IOException {
        logger.info("writing data to parquet writer");

        GenericData.Record record = new GenericData.Record(avroSchema);
        data.forEach(record::put);

        try {
            parquetWriter.write(record);
        } catch (Exception e) {
            logger.error("Error writing data to parquet writer with exception: {}", e.getMessage());
            throw e;
        }
        recordsWritten++;
    }

    /**
     * I process the written records convert it to bytes array and return it.
     *
     * @return byte array of the data
     * @throws IOException - Thrown if exception occur during processing written data
     */
    public byte[] toByteArray() throws IOException {
        try {
            parquetWriter.close();
        } catch (Exception e) {
            logger.error("Error while closing parquet writer with exception: {}", e.getMessage());
            throw e;
        }

        try {
            return parquetOutputFile.toByteArray();
        } catch (Exception e) {
            logger.error("Error getting the byte array from parquet output file with exception: {}", e.getMessage());
            throw e;
        }
    }


    private void instantiateNewParquetWriter(Schema avroSchema) throws IOException {
        logger.info("Instantiating new parquet writer. Setting records written to zero");

        try {
            recordsWritten = 0;
            parquetOutputFile = new ParquetOutputFile(new ByteArrayOutputStream());

            parquetWriter = AvroParquetWriter.<GenericData.Record>builder(parquetOutputFile)
                    .withSchema(avroSchema)
                    .build();

        } catch (Exception e) {
            logger.error("Error occurred while instantiating new parquet writer. Exception: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * I return the number of records buffered/written by the writer.
     *
     * @return Number of records written
     */
    public int recordsWritten() {
        return recordsWritten;
    }
}
