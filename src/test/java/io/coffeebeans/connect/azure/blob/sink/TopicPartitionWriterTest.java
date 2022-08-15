package io.coffeebeans.connect.azure.blob.sink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.coffeebeans.connect.azure.blob.sink.config.AzureBlobSinkConfig;
import io.coffeebeans.connect.azure.blob.sink.format.RecordWriter;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;


/**
 * Unit tests for TopicPartitionWriter.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TopicPartitionWriterTest {

    @Mock
    private RecordWriter recordWriter;

    @Mock
    private AzureBlobSinkConfig config;

    @Mock
    private AzureBlobSinkConnectorContext context;

    private TopicPartitionWriter topicPartitionWriter;

    /**
     * Mocking the classes used by the {@link TopicPartitionWriter}.
     *
     * @throws JsonProcessingException Thrown if encounters error while encoding partition.
     */
    @BeforeEach
    public void init() throws JsonProcessingException {

        // Mocking the call to get the config
        when(context.getConfig())
                .thenReturn(config);

        // Mocking the call to encode partition provided sink record
        when(context.encodePartition(any()))
                .thenReturn("env=test");

        // Mocking the call to generate full path provided sink record,
        // encoded partition and kafka starting offset
        when(context.generateFullPath(any(), anyString(), anyLong()))
                .thenReturn("topic/env=test/topic+partition+startingOffset");

        // Mocking call to get the record writer
        // provided kafka topic and output file name
        when(context.getRecordWriter(anyString(), anyString()))
                .thenReturn(recordWriter);

        // Mocking sending data to dead letter queue
        // provided sink record and the throwable
        doNothing()
                .when(context)
                .sendToDeadLetterQueue(any(), any());

        topicPartitionWriter = new TopicPartitionWriter(context);

        when(config.getFlushSize())
                .thenReturn(1);
    }

    /**
     * When very first record is buffered and write method is invoked, there should
     * be no exception thrown assuming {@link RecordWriter} writes the record
     * successfully.
     */
    @Test
    @DisplayName("Given first sink record, write method should write record successfully")
    void write_givenFirstSinkRecord_shouldWriteSuccessfully() {

        SinkRecord firstSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 1L
        );

        // Buffering the record
        topicPartitionWriter.buffer(firstSinkRecord);

        // Should not throw any exception and RecordWriter should be invoked only once
        assertDoesNotThrow(topicPartitionWriter::write);
        verify(recordWriter, times(1))
                .write(any());
    }

    /**
     * When very first record is buffered and write method is invoked, an exception
     * should be thrown assuming the {@link RecordWriter} throws exception
     * while writing the record.
     */
    @Test
    @DisplayName("Given first sink record, when record writer throws exception, "
            + "write method should write the record to dead letter queue")
    void write_givenFirstSinkRecord_whenRecordWriterThrowsException_shouldWriteRecordToDeadLetterQueue() {

        SinkRecord firstSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 1L
        );

        // Mocking the write method to throw IOException
        doThrow(RetriableException.class)
                .when(recordWriter)
                .write(any());

        // Buffering the record
        topicPartitionWriter.buffer(firstSinkRecord);

        assertDoesNotThrow(topicPartitionWriter::write);

        verify(context, times(1))
                .sendToDeadLetterQueue(any(), any());
    }

    /**
     * Unit test to check if the rotation is done when flush size
     * condition is met or not.
     */
    @Test
    @DisplayName("Given flush size as 1, with 4 sink records, "
            + "write method should invoke RecordWriter commit 3 times")
    void write_givenFlushSizeAsOne_withFourSinkRecords_shouldInvokeCommitThreeTimes() {

        SinkRecord firstSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 1L
        );

        SinkRecord secondSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 2L
        );

        SinkRecord thirdSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 3L
        );

        SinkRecord fourthSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 4L
        );

        /*
        For this test we need another instance of TopicPartitionWriter
        because the values are assigned in the constructor itself.
         */
        when(config.getFlushSize())
                .thenReturn(1);

        // This will disable check on interval
        when(config.getRotateIntervalMs())
                .thenReturn(-1L);

        // instantiate topic partition writer
        TopicPartitionWriter localTopicPartitionWriter = new TopicPartitionWriter(context);

        localTopicPartitionWriter.buffer(firstSinkRecord);
        localTopicPartitionWriter.buffer(secondSinkRecord);
        localTopicPartitionWriter.buffer(thirdSinkRecord);

        assertDoesNotThrow(localTopicPartitionWriter::write);

        /*
        Buffering and writing fourth record after first write attempt because
        condition for flush size will only be checked when a new record arrives or when
        the writer is closed.
         */
        localTopicPartitionWriter.buffer(fourthSinkRecord);
        assertDoesNotThrow(localTopicPartitionWriter::write);

        verify(recordWriter, times(3))
                .commit();
    }

    /**
     * Unit test to check if rotation is done when rotation.interval.ms
     * condition is met or not.
     *
     * @throws InterruptedException If exception thrown from Thread sleep method
     */
    @Test
    @DisplayName("Given rotation time interval ms as 1 ms, with 3 records, "
            + "assuming a time difference of 2 ms between write, should invoke "
            + "record writer commit 2 times")
    void write_givenRotationTimeIntervalAsOneMs_withThreeRecords_assumingTimeDifferenceOfTwoMsShouldInvokeCommitTwoTimes()
            throws InterruptedException {

        SinkRecord firstSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 1L
        );

        SinkRecord secondSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 2L
        );

        SinkRecord thirdSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 3L
        );

        /*
        For this test we need another instance of TopicPartitionWriter
        because the values are assigned in the constructor itself.
         */

        // This will disable check on flush size
        when(config.getFlushSize())
                .thenReturn(-1);

        // This will set interval to 1ms
        when(config.getRotateIntervalMs())
                .thenReturn(1L);

        // instantiate topic partition writer
        TopicPartitionWriter localTopicPartitionWriter = new TopicPartitionWriter(context);

        // Buffering and writing first record
        localTopicPartitionWriter.buffer(firstSinkRecord);
        assertDoesNotThrow(localTopicPartitionWriter::write);

        Thread.sleep(2); // Time difference of 2ms between consecutive writes

        // Buffering and writing second record
        localTopicPartitionWriter.buffer(secondSinkRecord);
        assertDoesNotThrow(localTopicPartitionWriter::write);

        Thread.sleep(2); // Time difference of 2ms between consecutive writes

        // Buffering and writing third record
        localTopicPartitionWriter.buffer(thirdSinkRecord);
        assertDoesNotThrow(localTopicPartitionWriter::write);

        Thread.sleep(2); // Time difference of 2ms between consecutive writes

        /*
        This final write will trigger the condition for checking time difference
        even if there is no record in the buffer.
         */
        assertDoesNotThrow(localTopicPartitionWriter::write);

        /*
        Total 2 interactions are expected because the first condition will only be
        checked when the 2nd record is successfully written.
        Then another condition check will happen when the third record is written.

        For the fourth write attempt, there is not writer present so no invocation is done.
         */
        verify(recordWriter, times(2))
                .commit();
    }

    /**
     * Unit test to check if commit throws an Exception, it should not
     * rethrow the exception.
     *
     * @throws InterruptedException If exception thrown from Thread sleep method
     */
    @Test
    @DisplayName("Given rotation time interval ms as 1 ms, with 3 records, "
            + "assuming a time difference of 2 ms between write, when encounters exception "
            + "should not rethrow exception")
    void write_givenRotationTimeIntervalAsOneMs_withThreeRecords_assumingTimeDifferenceOfTwoMs_whenEncountersException_shouldNotThrowException()
            throws InterruptedException {

        SinkRecord firstSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 1L
        );

        SinkRecord secondSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 2L
        );

        SinkRecord thirdSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 3L
        );

        /*
        For this test we need another instance of TopicPartitionWriter
        because the values are assigned in the constructor itself.
         */

        // This will disable check on flush size
        when(config.getFlushSize())
                .thenReturn(-1);

        // This will set interval to 1ms
        when(config.getRotateIntervalMs())
                .thenReturn(1L);

        // instantiate topic partition writer
        TopicPartitionWriter localTopicPartitionWriter = new TopicPartitionWriter(context);

        // Buffering and writing first record
        localTopicPartitionWriter.buffer(firstSinkRecord);
        assertDoesNotThrow(localTopicPartitionWriter::write);

        Thread.sleep(2); // Time difference of 2ms between consecutive writes

        // Buffering and writing second record
        localTopicPartitionWriter.buffer(secondSinkRecord);
        assertDoesNotThrow(localTopicPartitionWriter::write);

        Thread.sleep(2); // Time difference of 2ms between consecutive writes

        // Buffering and writing third record
        localTopicPartitionWriter.buffer(thirdSinkRecord);
        assertDoesNotThrow(localTopicPartitionWriter::write);

        Thread.sleep(2); // Time difference of 2ms between consecutive writes

        /*
        This final write will trigger the condition for checking time difference
        even if there is no record in the buffer.
         */
        assertDoesNotThrow(localTopicPartitionWriter::write);

        // Mocking it throw IOException while doing commit operation
        doThrow(RetriableException.class)
                .when(recordWriter)
                .commit();

        assertDoesNotThrow(localTopicPartitionWriter::close);
    }

    /**
     * Unit test to check if invoking close method in
     * {@link TopicPartitionWriter} invokes commit method
     * for all {@link RecordWriter}(s) or not.
     */
    @Test
    @DisplayName("Given one record, close method should invoke commit of record writer")
    void close_givenOneRecord_shouldInvokeCommitOfRecordWriter() {

        SinkRecord firstSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 1L
        );
        // Buffering the record
        topicPartitionWriter.buffer(firstSinkRecord);

        // Writing the record.
        assertDoesNotThrow(topicPartitionWriter::write);

        // Should not throw any exception and RecordWriter commit should be invoked only once
        assertDoesNotThrow(topicPartitionWriter::close);
        verify(recordWriter, times(1))
                .commit();
    }

    /**
     * Unit test to check if close method rethrows exception when
     * commit operation encounters an error.
     */
    @Test
    @DisplayName("When commit throws exception, close should rethrow exception")
    void close_whenCommitThrowsException_shouldRethrowException() {

        SinkRecord firstSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 1L
        );

        /*
        For this test we need another instance of TopicPartitionWriter
        because the values are assigned in the constructor itself.
         */

        // This will disable check on flush size
        when(config.getFlushSize())
                .thenReturn(-1);

        // This will disable check on interval
        when(config.getRotateIntervalMs())
                .thenReturn(-1L);

        // instantiate topic partition writer
        TopicPartitionWriter localTopicPartitionWriter = new TopicPartitionWriter(context);

        // Buffering the record
        localTopicPartitionWriter.buffer(firstSinkRecord);

        // Writing the record.
        assertDoesNotThrow(localTopicPartitionWriter::write);

        doThrow(RetriableException.class)
                .when(recordWriter)
                .commit();

        assertThrows(RetriableException.class, localTopicPartitionWriter::close);
    }

    /**
     * Unit tests to check if last successful offset is correct or not when
     * there is no exception thrown.
     */
    @Test
    @DisplayName("Given three records, when commit does not throw any exception, "
            + "getLastSuccessfulOffset should return last successful offset")
    void getLastSuccessfulOffset_givenThreeRecords_shouldReturnLastSuccessfulOffset_whenNoExceptionIsThrown() {

        SinkRecord firstSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 1L
        );

        SinkRecord secondSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 2L
        );

        SinkRecord thirdSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 3L
        );

        topicPartitionWriter.buffer(firstSinkRecord);
        topicPartitionWriter.buffer(secondSinkRecord);
        topicPartitionWriter.buffer(thirdSinkRecord);

        assertDoesNotThrow(topicPartitionWriter::write);

        assertEquals(
                thirdSinkRecord.kafkaOffset(),
                topicPartitionWriter.getLastSuccessfulOffset()
        );
    }

    /**
     * Unit tests to check if last successful offset returned is correct
     * or not when one of the record was faulty and exception was thrown by the writer.
     */
    @Test
    @DisplayName("Given 3 records, assuming 1 faulty record, getLastSuccessfulOffset "
            + "should return correct offset")
    void getLastSuccessfulOffset_givenThreeRecords_assumingOneFaultyRecord_shouldReturnCorrectLastSuccessfulOffset() {

        SinkRecord firstSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 1L
        );

        SinkRecord secondSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 2L
        );

        SinkRecord thirdSinkRecord = new SinkRecord(
                "TEST-TOPIC", 0, null, null,
                null, "TEST-VALUE", 3L
        );

        topicPartitionWriter.buffer(firstSinkRecord);
        topicPartitionWriter.buffer(secondSinkRecord);
        topicPartitionWriter.buffer(thirdSinkRecord);

        // Throw an exception for Second sink record
        doThrow(RetriableException.class)
                .when(recordWriter)
                .write(secondSinkRecord);

        assertDoesNotThrow(topicPartitionWriter::write);

        // Verify that Second record was sent to DLQ or not
        verify(context, times(1))
                .sendToDeadLetterQueue(any(), any());

        // Even though second record was faulty it will return the offset of last
        // SUCCESSFUL record written
        assertEquals(
                thirdSinkRecord.kafkaOffset(),
                topicPartitionWriter.getLastSuccessfulOffset()
        );
    }
}
