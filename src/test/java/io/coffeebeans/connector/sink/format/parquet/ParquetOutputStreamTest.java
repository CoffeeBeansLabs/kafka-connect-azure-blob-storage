package io.coffeebeans.connector.sink.format.parquet;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.coffeebeans.connector.sink.storage.StorageManager;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ParquetOutputStream.
 */
public class ParquetOutputStreamTest {

    private StorageManager storageManager;

    @Test
    @DisplayName("Given byte, when buffer size is not full write method should not invoke append method")
    void write_givenByte_whenBufferIsNotFull_shouldNotInvokeAppendMethod() throws IOException {

        String blobName = "test-blob";
        int partSize = 10;

        /*
        Part size is kept more than one byte so that when the output stream
        writes the byte into the buffer, there is still some space available.
         */

        storageManager = mock(StorageManager.class);
        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        int inputByte = 5;

        outputStream.write(inputByte);

        // StorageManager should not be invoked since buffer space is not full
        verifyNoInteractions(storageManager);

        // Position will be incremented by 1 position after writing.
        Assertions.assertEquals(1, outputStream.getPos());
    }

    @Test
    @DisplayName("Given byte, when buffer size is full write method should invoke append method exactly once")
    void write_givenByte_whenBufferIsFull_shouldInvokeAppendMethodExactlyOnce() throws IOException {

        String blobName = "test-blob";
        int partSize = 1;

        /*
        Part size is kept to 1 so that as soon as the output stream writes the
        given byte to the buffer, the space will be full, and it will need to flush
        it using the upload method thus invoking the append method of the storage manager.
         */

        storageManager = mock(StorageManager.class);
        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        int inputByte = 5;

        outputStream.write(inputByte);

        verify(storageManager, times(1));
        Assertions.assertEquals(1, outputStream.getPos());
    }

    @Test
    @DisplayName("Given null byte array, write should throw NullPointerException")
    void write_givenNullByteArray_shouldThrowNullPointerException() {

        String blobName = "test-blob";
        int partSize = 1;
        int offset = 3;
        int length = 6;

        storageManager = mock(StorageManager.class);
        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        Assertions.assertThrowsExactly(NullPointerException.class,
                () -> outputStream.write(null, offset, length));
    }

    @Test
    @DisplayName("Given offset more than byte array length, write should throw index out of bounds exception")
    void write_givenOffsetMoreThanByteArrayLength_shouldThrowIndexOutOfBoundsException() {

        String blobName = "test-blob";
        int partSize = 1;

        storageManager = mock(StorageManager.class);
        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        /*
        Invalid arguments set 1:
            Offset more than length of the byte array
         */
        byte[] bytesToBeWritten = new byte[10];
        int offset = 15;
        int length = 5;

        Assertions.assertThrowsExactly(IndexOutOfBoundsException.class,
                () -> outputStream.write(bytesToBeWritten, offset, length));
    }

    @Test
    @DisplayName("Given length less than zero, write should throw index out of bounds exception")
    void write_givenLengthLessThanZero_shouldThrowIndexOutOfBoundsException() {

        String blobName = "test-blob";
        int partSize = 1;

        storageManager = mock(StorageManager.class);
        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        /*
        Invalid arguments set 2:
            Length less than zero
         */
        byte[] bytesToBeWritten = new byte[10];
        int offset = 5;
        int length = -1;

        Assertions.assertThrowsExactly(IndexOutOfBoundsException.class,
                () -> outputStream.write(bytesToBeWritten, offset, length));
    }

    @Test
    @DisplayName("Given sum of length and offset more than byte array length, write should "
            + "throw index out of bounds exception")
    void write_givenSumOfLengthAndOffsetMoreThanByteArrayLength_shouldThrowIndexOutOfBoundsException() {

        String blobName = "test-blob";
        int partSize = 1;

        storageManager = mock(StorageManager.class);
        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        /*
        Invalid arguments set 3:
            Sum of offset and length more than byte array length
         */
        byte[] bytesToBeWritten = new byte[10];
        int offset = 6;
        int length = 5;

        Assertions.assertThrowsExactly(IndexOutOfBoundsException.class,
                () -> outputStream.write(bytesToBeWritten, offset, length));
    }

    @Test
    @DisplayName("Given bytes three times more than buffer size, should invoke storage manager three times")
    void write_givenBytesThreeTimesMoreThanBufferSize_shouldInvokeStorageManagerThreeTimes() throws IOException {

        String blobName = "test-blob";
        int partSize = 3;

        storageManager = mock(StorageManager.class);
        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        byte[] bytesToBeWritten = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        int offset = 0;
        int length = 9;

        outputStream.write(bytesToBeWritten, offset, length);

        verify(storageManager, times(3))
                .append(any(), any());

        Assertions.assertEquals(9, outputStream.getPos());
    }

    @Test
    @DisplayName("Given commit flag set to true and non-empty buffer, close method should invoke storage manager")
    void close_givenCommitFlagTrueAndBufferNotEmpty_shouldInvokeStorageManager() throws IOException {

        String blobName = "test-blob";
        int partSize = 3;

        storageManager = mock(StorageManager.class);
        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        byte[] bytesToBeWritten = {1, 2};
        int offset = 0;
        int length = 2;

        /*
        write method will not invoke the storage manager as the buffer size is not full yet.
         */
        outputStream.write(bytesToBeWritten, offset, length);

        // Setting the commit flag to true
        outputStream.setCommitFlag(true);

        /*
        Triggering close method should invoke storage manager to upload the remaining data.
         */
        outputStream.close();

        verify(storageManager, times(1))
                .append(any(), any());
    }
}
