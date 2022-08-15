package io.coffeebeans.connect.azure.blob.sink.format.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.coffeebeans.connect.azure.blob.sink.storage.StorageManager;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

/**
 * Unit tests for {@link ParquetOutputStream}.
 */
@ExtendWith(MockitoExtension.class)
public class ParquetOutputStreamTest {

    @Mock
    private StorageManager storageManager;

    /**
     * <b>Method: {@link ParquetOutputStream#write(int)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Given byte</li>
     *     <li>Buffer is not full</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should not invoke {@link StorageManager}</li>
     * </ul>
     */
    @Test
    @DisplayName("Given byte, when buffer size is not full write method should not invoke storage manager")
    void write_givenByte_whenBufferIsNotFull_shouldNotInvokeStorageManager() throws IOException {

        String blobName = "test-blob";
        int partSize = 10;

        /*
        Part size is kept more than one byte so that when the output stream
        writes the byte into the buffer, there is still some space available.
         */

        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        int inputByte = 5;

        outputStream.write(inputByte);

        // StorageManager should not be invoked since buffer space is not full
        verifyNoInteractions(storageManager);

        // Position will be incremented by 1 position after writing.
        assertEquals(1, outputStream.getPos());
    }

    /**
     * <b>Method: {@link ParquetOutputStream#write(int)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Given byte</li>
     *     <li>Buffer is full</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should invoke {@link StorageManager}</li>
     * </ul>
     */
    @Test
    @DisplayName("Given byte, when buffer size is full write method should invoke storage manager")
    void write_givenByte_whenBufferIsFull_shouldInvokeStorageManager() throws IOException {

        String blobName = "test-blob";
        int partSize = 1;

        /*
        Part size is kept to 1 so that as soon as the output stream writes the
        given byte to the buffer, the space will be full, and it will need to flush
        it using the upload method thus invoking the append method of the storage manager.
         */

        when(storageManager.stageBlockAsync(anyString(), anyString(), any()))
                .thenReturn(Mono.empty());

        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        int inputByte = 5;

        outputStream.write(inputByte);

        verify(storageManager, times(1))
                .stageBlockAsync(anyString(), anyString(), any());

        assertEquals(1, outputStream.getPos());
    }

    /**
     * <b>Method: {@link ParquetOutputStream#write(byte[], int, int)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Given null byte array</li>
     *     <li>Buffer is not full</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>{@link ParquetOutputStream#write(byte[], int, int)} should throw<br>
     *     {@link NullPointerException}</li>
     * </ul>
     */
    @Test
    @DisplayName("Given null byte array, write should throw NullPointerException")
    void write_givenNullByteArray_shouldThrowNullPointerException() {

        String blobName = "test-blob";
        int partSize = 1;
        int offset = 3;
        int length = 6;

        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        assertThrowsExactly(NullPointerException.class,
                () -> outputStream.write(null, offset, length));
    }

    /**
     * <b>Method: {@link ParquetOutputStream#write(byte[], int, int)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Given offset > length</li>
     *     <li>Buffer is not full</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>{@link ParquetOutputStream#write(byte[], int, int)} should <br>
     *     throw {@link IndexOutOfBoundsException}</li>
     * </ul>
     */
    @Test
    @DisplayName("Given offset more than byte array length, write should throw index out of bounds exception")
    void write_givenOffsetMoreThanByteArrayLength_shouldThrowIndexOutOfBoundsException() {

        String blobName = "test-blob";
        int partSize = 1;

        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        /*
        Invalid arguments set 1:
            Offset more than length of the byte array
         */
        byte[] bytesToBeWritten = new byte[10];
        int offset = 15;
        int length = 5;

        assertThrowsExactly(IndexOutOfBoundsException.class,
                () -> outputStream.write(bytesToBeWritten, offset, length));
    }

    /**
     * <b>Method: {@link ParquetOutputStream#write(byte[], int, int)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Given length < 0</li>
     *     <li>Buffer is not full</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>{@link ParquetOutputStream#write(byte[], int, int)} should <br>
     *     throw {@link IndexOutOfBoundsException}</li>
     * </ul>
     */
    @Test
    @DisplayName("Given length less than zero, write should throw index out of bounds exception")
    void write_givenLengthLessThanZero_shouldThrowIndexOutOfBoundsException() {

        String blobName = "test-blob";
        int partSize = 1;

        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        /*
        Invalid arguments set 2:
            Length less than zero
         */
        byte[] bytesToBeWritten = new byte[10];
        int offset = 5;
        int length = -1;

        assertThrowsExactly(IndexOutOfBoundsException.class,
                () -> outputStream.write(bytesToBeWritten, offset, length));
    }

    /**
     * <b>Method: {@link ParquetOutputStream#write(byte[], int, int)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Given length + offset > array length</li>
     *     <li>Buffer is not full</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>{@link ParquetOutputStream#write(byte[], int, int)} should <br>
     *     throw {@link IndexOutOfBoundsException}</li>
     * </ul>
     */
    @Test
    @DisplayName("Given sum of length and offset more than byte array length, write should "
            + "throw index out of bounds exception")
    void write_givenSumOfLengthAndOffsetMoreThanByteArrayLength_shouldThrowIndexOutOfBoundsException() {

        String blobName = "test-blob";
        int partSize = 1;

        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        /*
        Invalid arguments set 3:
            Sum of offset and length more than byte array length
         */
        byte[] bytesToBeWritten = new byte[10];
        int offset = 6;
        int length = 5;

        assertThrowsExactly(IndexOutOfBoundsException.class,
                () -> outputStream.write(bytesToBeWritten, offset, length));
    }

    /**
     * <b>Method: {@link ParquetOutputStream#write(byte[], int, int)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Given byte 3 times more than buffer size</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should invoke {@link StorageManager} 3 times</li>
     * </ul>
     */
    @Test
    @DisplayName("Given bytes three times more than buffer size, should invoke storage manager three times")
    void write_givenBytesThreeTimesMoreThanBufferSize_shouldInvokeStorageManagerThreeTimes() throws IOException {

        String blobName = "test-blob";
        int partSize = 3;

        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        byte[] bytesToBeWritten = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        int offset = 0;
        int length = 9;

        // Mocking staging and commit api request
        when(storageManager.stageBlockAsync(anyString(), anyString(), any()))
                .thenReturn(Mono.empty());

        outputStream.write(bytesToBeWritten, offset, length);

        verify(storageManager, times(3))
                .stageBlockAsync(anyString(), anyString(), any());

        assertEquals(9, outputStream.getPos());
    }

    /**
     * <b>Method: {@link ParquetOutputStream#write(byte[], int, int)}</b>.<br>
     * <b>Assumptions: </b>
     * <ul>
     *     <li>Commit flag set to true</li>
     *     <li>Non-empty buffer</li>
     * </ul>
     *
     * <p><b>Expectations: </b>
     * <ul>
     *     <li>Should invoke {@link StorageManager}</li>
     * </ul>
     */
    @Test
    @DisplayName("Given commit flag set to true and non-empty buffer, close method should invoke storage manager")
    void close_givenCommitFlagTrueAndBufferNotEmpty_shouldInvokeStorageManager() throws IOException {

        String blobName = "test-blob";
        int partSize = 3;

        ParquetOutputStream outputStream = new ParquetOutputStream(storageManager, blobName, partSize);

        byte[] bytesToBeWritten = {1, 2};
        int offset = 0;
        int length = 2;

        // Mocking staging and commit api request
        when(storageManager.stageBlockAsync(anyString(), anyString(), any()))
                .thenReturn(Mono.empty());
        when(storageManager.commitBlockIdsAsync(anyString(), any(), anyBoolean()))
                .thenReturn(Mono.empty());

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

        // 1 for staging & 1 for commit
        verify(storageManager, times(1))
                .stageBlockAsync(anyString(), anyString(), any());

        verify(storageManager, times(1))
                .commitBlockIdsAsync(anyString(), any(), anyBoolean());
    }
}
