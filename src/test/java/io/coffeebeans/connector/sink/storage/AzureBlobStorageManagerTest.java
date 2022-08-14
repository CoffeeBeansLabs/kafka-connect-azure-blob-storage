package io.coffeebeans.connector.sink.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.rest.Response;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.AppendBlobItem;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.AppendBlobCreateOptions;
import com.azure.storage.blob.specialized.AppendBlobAsyncClient;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import io.coffeebeans.connector.sink.exception.BlobStorageException;
import io.coffeebeans.connector.sink.exception.UnsupportedOperationException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;


/**
 * Unit tests for AzureBlobStorageManager.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AzureBlobStorageManagerTest {
    private static final String CONN_STR_VALUE = "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuF"
            + "q2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProt"
            + "ocol=http;BlobEndpoint=http://host.docker.internal:10000/dev"
            + "storeaccount1;";

    private static final String CONTAINER_NAME = "test-container";

    @Mock
    private BlobClient blobClient;

    @Mock
    private AppendBlobClient appendBlobClient;

    @Mock
    private BlobContainerClient blobContainerClient;

    @Mock
    private BlobContainerAsyncClient blobContainerAsyncClient;

    @Mock
    private BlobAsyncClient blobAsyncClient;

    @Mock
    private AppendBlobAsyncClient appendBlobAsyncClient;

    @Mock
    private BlockBlobAsyncClient blockBlobAsyncClient;

    @Mock
    private Response mockedResponse;

    private final AzureBlobStorageManager azureBlobStorageManager = new AzureBlobStorageManager(
            CONN_STR_VALUE, CONTAINER_NAME
    );

    /**
     * It will set the blob container client field
     * inside the AzureBlobStorageManager via reflection.
     * It mocks container client to return mocked blob and
     * append blob client.
     *
     * @throws Exception If encounters any issue while injecting mock instance
     */
    @BeforeEach
    void init() throws Exception {

        /*
        Used for synchronous call to blob storage.
         */

        // Injecting the mocked BlobContainerClient instance via Reflection
        Field blobContainerClientField = azureBlobStorageManager
                .getClass()
                .getDeclaredField("containerClient");

        blobContainerClientField.setAccessible(true);
        blobContainerClientField.set(azureBlobStorageManager, blobContainerClient);

        // BlobContainerClient mocks
        when(blobContainerClient.getBlobClient(anyString()))
                .thenReturn(blobClient);

        when(blobClient.getAppendBlobClient())
                .thenReturn(appendBlobClient);


        /*
        Used for asynchronous call to blob storage.
         */

        // Injecting the mocked BlobContainerAsyncClient instance via Reflection
        Field blobContainerAsyncClientField = azureBlobStorageManager
                .getClass()
                .getDeclaredField("containerAsyncClient");

        blobContainerAsyncClientField.setAccessible(true);
        blobContainerAsyncClientField.set(azureBlobStorageManager, blobContainerAsyncClient);

        // BlobContainerAsyncClient mocks
        when(blobContainerAsyncClient.getBlobAsyncClient(anyString()))
                .thenReturn(blobAsyncClient);

        when(blobAsyncClient.getAppendBlobAsyncClient())
                .thenReturn(appendBlobAsyncClient);

        when(blobAsyncClient.getBlockBlobAsyncClient())
                .thenReturn(blockBlobAsyncClient);
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#AzureBlobStorageManager(String, String)}</b>.<br>
     * <b>Assumptions: </b><br>
     * <ul>
     *     <li>Valid connection string</li>
     *     <li>Valid container name</li>
     * </ul>
     *
     * <p><b>Expectations: </b><br>
     * <ul>
     *     <li>Should create a new object</li>
     * </ul>
     */
    @Test
    @DisplayName("Given correct connection string and container name, should create a new object")
    void givenCorrectConnectionStringAndContainerName_shouldCreateNewObject() {

        assertDoesNotThrow(
                () -> new AzureBlobStorageManager(CONN_STR_VALUE, CONTAINER_NAME)
        );
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#AzureBlobStorageManager(String, String)}</b>.<br>
     * <b>Assumptions: </b><br>
     * <ul>
     *     <li>Invalid connection string</li>
     *     <li>Valid container name</li>
     * </ul>
     *
     * <p><b>Expectations: </b><br>
     * <ul>
     *     <li>Should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given invalid connection string and valid container name, should throw exception")
    void givenInvalidConnectionStringAndValidContainerName_shouldThrowException() {

        String invalidConnectionString = "http://localhost/invalid";

        assertThrows(Exception.class,
                () -> new AzureBlobStorageManager(
                        invalidConnectionString, CONTAINER_NAME
                )
        );
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#AzureBlobStorageManager(String, String)}</b>.<br>
     * <b>Assumptions: </b><br>
     * <ul>
     *     <li>Valid connection string</li>
     *     <li>Invalid container name</li>
     * </ul>
     *
     * <p><b>Expectations: </b><br>
     * <ul>
     *     <li>Should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given valid connection string and invalid container name, should throw exception")
    void givenValidConnectionStringAndInvalidContainerName_shouldThrowException() {

        String invalidContainerName = "$%&# \n  ";

        assertThrows(Exception.class,
                () -> new AzureBlobStorageManager(
                        CONN_STR_VALUE, invalidContainerName
                )
        );
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#append(String, byte[])}</b>.<br>
     * <b>Assumptions: </b><br>
     * <ul>
     *     <li>Append blob -> Does not exist</li>
     * </ul>
     *
     * <p><b>Expectations: </b><br>
     * <ul>
     *     <li>Creates an Append blob</li>
     *     <li>Appends data to the Append blob</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, when append blob does not exist, "
            + "append method should create blob and append data")
    void append_givenBlobNameAndData_whenAppendBlobDoesNotExist_shouldCreateBlobAndAppendData() {

        String blobName = "test-blob";
        byte[] data = {0, 1, 2, 3};

        // Mocking to return response indicating blob does not exist
        when(appendBlobClient.exists())
                .thenReturn(false);

        // Mocking blob creation api call
        when(appendBlobClient.createWithResponse(any(), any(), any()))
                .thenReturn(mockedResponse);

        // Mocking append data api call
        when(appendBlobClient
                .appendBlockWithResponse(any(), anyLong(), any(), any(), any(), any()))
                .thenReturn(mockedResponse);


        azureBlobStorageManager.append(blobName, data);

        // Verifying blob creation api call
        verify(appendBlobClient, times(1))
                .createWithResponse(any(), any(), any());

        // Verifying data append api call
        verify(appendBlobClient, times(1))
                .appendBlockWithResponse(any(), anyLong(), any(), any(), any(), any());
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#append(String, byte[])}</b>.<br>
     * <b>Assumptions: </b><br>
     * <ul>
     *     <li>Append blob does not exist</li>
     *     <li>Create request fails</li>
     * </ul>
     *
     * <p><b>Expectations: </b><br>
     * <ul>
     *     <li>{@link AzureBlobStorageManager#append(String, byte[])} throws an exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, when append blob does not exist, "
            + "append method should throw exception assuming create operation fails")
    void append_givenBlobNameAndData_whenAppendBlobDoesNotExist_shouldThrowException_assumingCreateRequestFails() {

        String blobName = "test-blob";
        byte[] data = {0, 1, 2, 3};

        // Mocking to return response indicating blob does not exist
        when(appendBlobClient.exists())
                .thenReturn(false);

        // Mocking blob creation api call
        when(appendBlobClient.createWithResponse(any(), any(), any()))
                .thenThrow(RuntimeException.class);

        assertThrows(RuntimeException.class,
                () -> azureBlobStorageManager.append(blobName, data));
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#append(String, byte[])}</b>.<br>
     * <b>Assumptions: </b><br>
     * <ul>
     *     <li>Append blob exist</li>
     * </ul>
     *
     * <p><b>Expectations: </b><br>
     * <ul>
     *     <li>Should not request blob creation API</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, when append blob exist, "
            + "append method should not try to create append blob")
    void append_givenBlobNameAndData_whenAppendBlobExist_shouldNotTryToCreateBlob() {

        String blobName = "test-blob";
        byte[] data = {0, 1, 2, 3};

        // Mocking to return response indicating blob does not exist
        when(appendBlobClient.exists())
                .thenReturn(true);

        // Mocking append data api call
        when(appendBlobClient
                .appendBlockWithResponse(any(), anyLong(), any(), any(), any(), any()))
                .thenReturn(mockedResponse);


        azureBlobStorageManager.append(blobName, data);

        // Verifying blob creation api call
        verify(appendBlobClient, times(0))
                .createWithResponse(any(), any(), any());

        // Verifying data append api call
        verify(appendBlobClient, times(1))
                .appendBlockWithResponse(any(), anyLong(), any(), any(), any(), any());
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#append(String, byte[])}</b>.<br>
     * <b>Assumptions: </b><br>
     * <ul>
     *     <li>Append blob does not exist</li>
     *     <li>Blob max size not provided</li>
     * </ul>
     *
     * <p><b>Expectations: </b><br>
     * <ul>
     *     <li>Blob max size request parameter should be null</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, when append blob does not exist , "
            + "and blob size not provided, append blob max size should not be specified")
    void append_givenBlobNameAndData_whenAppendBlobDoesNotExistAndMaxSizeNotProvided_maxBlobSizeShouldNotBeSpecified() {

        String blobName = "test-blob";
        byte[] data = {0, 1, 2, 3};

        ArgumentCaptor<AppendBlobCreateOptions> createOptionsArgumentCaptor = ArgumentCaptor
                .forClass(AppendBlobCreateOptions.class);

        // Mocking to return response indicating blob does not exist
        when(appendBlobClient.exists())
                .thenReturn(false);

        // Mocking blob creation api call
        when(appendBlobClient
                .createWithResponse(createOptionsArgumentCaptor.capture(), any(), any())
        ).thenReturn(mockedResponse);

        // Mocking append data api call
        when(appendBlobClient
                .appendBlockWithResponse(any(), anyLong(), any(), any(), any(), any()))
                .thenReturn(mockedResponse);


        azureBlobStorageManager.append(blobName, data);

        // Verifying blob creation api call
        verify(appendBlobClient, times(1))
                .createWithResponse(any(), any(), any());

        AppendBlobCreateOptions capturedCreateOptionsArgument =
                createOptionsArgumentCaptor.getValue();

        AppendBlobRequestConditions appendBlobRequestConditions = (AppendBlobRequestConditions)
                capturedCreateOptionsArgument.getRequestConditions();

        Long maxBlobSize = appendBlobRequestConditions.getMaxSize();

        // max size is set to null when not explicitly set.
        assertNull(maxBlobSize);
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#append(String, byte[])}</b>.<br>
     * <b>Assumptions: </b><br>
     * <ul>
     *     <li>Append blob does not exist</li>
     *     <li>Blob max size provided</li>
     * </ul>
     *
     * <p><b>Expectations: </b><br>
     * <ul>
     *     <li>Blob max size should be provided in the request parameter</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, when append blob does not exist , "
            + "and blob size provided, append blob max size should be specified")
    void append_givenBlobNameAndData_whenAppendBlobDoesNotExistAndMaxSizeProvided_maxBlobSizeShouldBeSpecified() {

        String blobName = "test-blob";
        byte[] data = {0, 1, 2, 3};
        Long providedMaxBlobSize = 100L;

        ArgumentCaptor<AppendBlobCreateOptions> createOptionsArgumentCaptor = ArgumentCaptor
                .forClass(AppendBlobCreateOptions.class);

        // Mocking to return response indicating blob does not exist
        when(appendBlobClient.exists())
                .thenReturn(false);

        // Mocking blob creation api call
        when(appendBlobClient
                .createWithResponse(createOptionsArgumentCaptor.capture(), any(), any())
        ).thenReturn(mockedResponse);

        // Mocking append data api call
        when(appendBlobClient
                .appendBlockWithResponse(any(), anyLong(), any(), any(), any(), any()))
                .thenReturn(mockedResponse);


        azureBlobStorageManager.append(blobName, providedMaxBlobSize, data);

        // Verifying blob creation api call
        verify(appendBlobClient, times(1))
                .createWithResponse(any(), any(), any());

        AppendBlobCreateOptions capturedCreateOptionsArgument =
                createOptionsArgumentCaptor.getValue();

        AppendBlobRequestConditions appendBlobRequestConditions = (AppendBlobRequestConditions)
                capturedCreateOptionsArgument.getRequestConditions();

        Long capturedMaxBlobSize = appendBlobRequestConditions.getMaxSize();

        // max size is set to null when not explicitly set.
        assertEquals(providedMaxBlobSize, capturedMaxBlobSize);
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#append(String, byte[])}</b>.<br>
     * <b>Assumptions: </b><br>
     * <ul>
     *     <li>Append blob exist</li>
     *     <li>Data append request fails</li>
     * </ul>
     *
     * <p><b>Expectations: </b><br>
     * <ul>
     *     <li>{@link AzureBlobStorageManager#append(String, byte[])} should throw exception</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, when append blob exist, "
            + "append method should throw exception assuming append request fails")
    void append_givenBlobNameAndData_whenAppendBlobExist_shouldThrowException_assumingAppendRequestFails() {

        String blobName = "test-blob";
        byte[] data = {0, 1, 2, 3};

        // Mocking to return response indicating blob does not exist
        when(appendBlobClient.exists())
                .thenReturn(true);

        // Mocking append request api call
        when(appendBlobClient
                .appendBlockWithResponse(any(), anyLong(), any(), any(), any(), any()))
                .thenThrow(RuntimeException.class);

        assertThrows(RuntimeException.class,
                () -> azureBlobStorageManager.append(blobName, data));
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#upload(String, byte[])}</b>.<br>
     *
     * <p><b>Expectations: </b><br>
     * <ul>
     *     <li>{@link AzureBlobStorageManager#upload(String, byte[])} should
     *     throw {@link UnsupportedOperationException}</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, upload method should throw unsupported exception")
    void upload_givenBlobNameAndData_shouldThrowUnsupportedException() {

        String blobName = "test-blob";
        byte[] data = {0, 1, 2, 3};

        assertThrows(UnsupportedOperationException.class,
                () -> azureBlobStorageManager.upload(blobName, data));
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#upload(String, long, byte[])}</b>.<br>
     *
     * <p><b>Expectations: </b><br>
     * <ul>
     *     <li>{@link AzureBlobStorageManager#upload(String, long, byte[])} should
     *     throw {@link UnsupportedOperationException}</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, with max blob size, "
            + "upload method should throw unsupported exception")
    void upload_givenBlobNameAndData_withMaxBlobSize_shouldThrowUnsupportedException() {

        String blobName = "test-blob";
        byte[] data = {0, 1, 2, 3};
        long maxBlobSize = 100L;

        assertThrows(UnsupportedOperationException.class,
                () -> azureBlobStorageManager.upload(blobName, maxBlobSize, data));
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#appendAsync(String, byte[])}</b>.<br>
     * <b>Assumptions:</b><br>
     * <ul>
     *     <li>Blob name is valid</li>
     *     <li>Byte array is valid</li>
     *     <li>Append blob does not exist</li>
     *     <li>Creating append blob was successful and returned status code 201</li>
     *     <li>Append block was successful</li>
     * </ul>
     *
     * <p><b>Expectations:</b><br>
     * <ul>
     *     <li>Should return complete signal</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, when blob does not exist, appendAsync method "
            + "should create blob and append data")
    void appendAsync_givenBlobNameAndByteArray_whenBlobDoesNotExist_shouldCreateBlobAndAppendData() {

        // Testing with default retry values
        azureBlobStorageManager.configure(new HashMap<>());

        String blobName = "test-blob";
        byte[] data = {1, 2, 3, 1, 5, 2};

        Response<AppendBlobItem> mockedResponseForCreate = getMockResponse(201);
        Mono<Response<AppendBlobItem>> mockedResponseForCreateMono = Mono.just(mockedResponseForCreate);

        Response<AppendBlobItem> mockedResponseForAppend = getMockResponse(200);
        Mono<Response<AppendBlobItem>> mockedResponseForAppendMono = Mono.just(mockedResponseForAppend);

        // Mocking sdk calls
        when(appendBlobAsyncClient.createIfNotExistsWithResponse(any()))
                .thenReturn(mockedResponseForCreateMono);

        when(appendBlobAsyncClient.appendBlockWithResponse(any(), anyLong(), any(), any()))
                .thenReturn(mockedResponseForAppendMono);

        Mono<Response<AppendBlobItem>> appendAsyncResponse = azureBlobStorageManager
                .appendAsync(blobName, data);


        StepVerifier.create(appendAsyncResponse)
                .then(appendAsyncResponse::subscribe)
                .expectNext(mockedResponseForAppend)
                .verifyComplete();


        // Verifying SDK calls
        verify(appendBlobAsyncClient, times(1))
                .createIfNotExistsWithResponse(any());

        verify(appendBlobAsyncClient, times(1))
                .appendBlockWithResponse(any(), anyLong(), any(), any());
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#appendAsync(String, byte[])}</b>.<br>
     * <b>Assumption:</b><br>
     * <ul>
     *     <li>Blob name is valid</li>
     *     <li>Data is valid and non null or empty</li>
     *     <li>Append blob already exist</li>
     *     <li>Append blob create request will return status 409</li>
     *     <li>Append request will return success</li>
     * </ul>
     *
     * <p><b>Expectations:</b><br>
     * <ul>
     *     <li>Should return complete signal with correct response</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, when blob already exist, append async should append data")
    void appendAsync_givenBlobNameAndByteArray_whenBlobExist_shouldAppendData() {

        // Testing with default retry values
        azureBlobStorageManager.configure(new HashMap<>());

        String blobName = "test-blob";
        byte[] data = {1, 2, 3, 1, 5, 2};

        Response<AppendBlobItem> mockedResponseForCreate = getMockResponse(409);
        Mono<Response<AppendBlobItem>> mockedResponseForCreateMono = Mono.just(mockedResponseForCreate);

        Response<AppendBlobItem> mockedResponseForAppend = getMockResponse(200);
        Mono<Response<AppendBlobItem>> mockedResponseForAppendMono = Mono.just(mockedResponseForAppend);

        // Mocking sdk calls
        when(appendBlobAsyncClient.createIfNotExistsWithResponse(any()))
                .thenReturn(mockedResponseForCreateMono);

        when(appendBlobAsyncClient.appendBlockWithResponse(any(), anyLong(), any(), any()))
                .thenReturn(mockedResponseForAppendMono);

        Mono<Response<AppendBlobItem>> appendAsyncResponse = azureBlobStorageManager
                .appendAsync(blobName, data);


        StepVerifier.create(appendAsyncResponse)
                .then(appendAsyncResponse::subscribe)
                .expectNext(mockedResponseForAppend)
                .verifyComplete();


        // Verifying SDK calls
        verify(appendBlobAsyncClient, times(1))
                .createIfNotExistsWithResponse(any());

        verify(appendBlobAsyncClient, times(1))
                .appendBlockWithResponse(any(), anyLong(), any(), any());
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#appendAsync(String, byte[])}</b>.<br>
     * <b>Assumption:</b><br>
     * <ul>
     *     <li>Blob name is valid</li>
     *     <li>Data is valid and non null or empty</li>
     *     <li>Append blob does not exist</li>
     *     <li>Append blob create request will return mono of error</li>
     * </ul>
     *
     * <p><b>Expectations:</b><br>
     * <ul>
     *     <li>Should retry for given number of retries (default is 3)</li>
     *     <li>{@link AzureBlobStorageManager#appendAsync(String, byte[])} should throw
     *     {@link io.coffeebeans.connector.sink.exception.BlobStorageException}</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, when blob does not exist, "
            + "assuming create request failed, append async should throw BlobStorageException")
    void appendAsync_givenBlobNameAndByteArray_whenBlobDoesNotExist_assumingCreateRequestFails_shouldThrowException() {

        // Testing with default retry values
        azureBlobStorageManager.configure(new HashMap<>());

        String blobName = "test-blob";
        byte[] data = {1, 2, 3, 1, 5, 2};
        AtomicInteger failedResponseCount = new AtomicInteger(0);

        /*
        Mocked create method to return mono of error
         */
        HttpResponse mockedHttpResponse = getMockHttpResponse();
        Mono<Response<AppendBlobItem>> mockedResponseForCreateMono = Mono.error(
                new com.azure.storage.blob.models.BlobStorageException("mocked", mockedHttpResponse, null));

        // Increment the counter for all failed requests (includes tries)
        Mono<Response<AppendBlobItem>> retryCountMockedMono = mockedResponseForCreateMono
                .doOnError(com.azure.storage.blob.models.BlobStorageException.class,
                        t -> failedResponseCount.getAndIncrement());

        /*
        Mocked append method to return mocked response. This response is not significant
        as it is not asserted.
         */
        Response<AppendBlobItem> mockedResponseForAppend = getMockResponse(404);
        Mono<Response<AppendBlobItem>> mockedResponseForAppendMono = Mono.just(mockedResponseForAppend);

        // Mocking sdk calls
        when(appendBlobAsyncClient.createIfNotExistsWithResponse(any()))
                .thenReturn(retryCountMockedMono);

        when(appendBlobAsyncClient.appendBlockWithResponse(any(), anyLong(), any(), any()))
                .thenReturn(mockedResponseForAppendMono);

        // Action
        Mono<Response<AppendBlobItem>> appendAsyncResponse = azureBlobStorageManager
                .appendAsync(blobName, data);

        /*
        Default values:
        Retries: 3
        Backoff ms: 4000 (4s)
        Max backoff ms: 120000 (120s)
        Connection timeout ms: 30000 (30s)
         */
        StepVerifier.withVirtualTime(() -> appendAsyncResponse)
                .thenAwait(Duration.ofSeconds(120)) // Waiting up to max backoff
                .expectError(BlobStorageException.class)
                .verify();

        // Total number of performed retries
        assertEquals(4, failedResponseCount.get()); // 1 try and 3 retries

        // Verifying SDK calls
        verify(appendBlobAsyncClient, times(1))
                .createIfNotExistsWithResponse(any());

        verify(appendBlobAsyncClient, times(1))
                .appendBlockWithResponse(any(), anyLong(), any(), any());
    }


    /**
     * <b>Method: {@link AzureBlobStorageManager#appendAsync(String, byte[])}</b>.<br>
     * <b>Assumption:</b><br>
     * <ul>
     *     <li>Blob name is valid</li>
     *     <li>Data is valid and non null or empty</li>
     *     <li>Append blob does not exist</li>
     *     <li>Append blob create request returns mono of error for 1st try and 2 retries</li>
     *     <li>3rd retry is successful</li>
     * </ul>
     *
     * <p><b>Expectations:</b><br>
     * <ul>
     *     <li>Should retry for given number of retries (default is 3)</li>
     *     <li>{@link AzureBlobStorageManager#appendAsync(String, byte[])} should
     *     return complete signal</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and data, when blob does not exist, "
            + "assuming create request failed for 1st try and 2 retries, "
            + "and 3rd retry is successful, append async method should return complete signal")
    void appendAsync_givenBlobNameAndByteArray_withSuccessfulRetry_shouldRetryCompleteSignal() {

        // Testing with default retry values
        azureBlobStorageManager.configure(new HashMap<>());

        String blobName = "test-blob";
        byte[] data = {1, 2, 3, 1, 5, 2};
        AtomicInteger failedResponseCount = new AtomicInteger(0);

        Response<AppendBlobItem> mockedResponseForCreate = getMockResponse(201);
        Mono<Response<AppendBlobItem>> mockedResponseForCreateMono = Mono.just(mockedResponseForCreate);

        /*
        Mocked create method to return mono of error
         */
        HttpResponse mockedHttpResponse = getMockHttpResponse();
        Mono<Response<AppendBlobItem>> mockMonoError = Mono.error(
                new com.azure.storage.blob.models.BlobStorageException("mocked", mockedHttpResponse, null));

        // Increment the counter for all failed requests (includes tries)
        // Check for the number of retries. If it is 3 (i.e 1 try and 2 retries), it should return
        // correct response.

        Mono<Response<AppendBlobItem>> retryCountMockedMono = mockedResponseForCreateMono
                .flatMap(response -> {
                    if (failedResponseCount.get() != 3) {
                        return mockMonoError;
                    }
                    return mockedResponseForCreateMono;
                })
                .doOnError(com.azure.storage.blob.models.BlobStorageException.class,
                        t -> failedResponseCount.getAndIncrement());

        /*
        Mocked append method to return mocked response. This response is not significant
        as it is not asserted.
         */
        Response<AppendBlobItem> mockedResponseForAppend = getMockResponse(200);
        Mono<Response<AppendBlobItem>> mockedResponseForAppendMono = Mono.just(mockedResponseForAppend);

        // Mocking sdk calls
        when(appendBlobAsyncClient.createIfNotExistsWithResponse(any()))
                .thenReturn(retryCountMockedMono);

        when(appendBlobAsyncClient.appendBlockWithResponse(any(), anyLong(), any(), any()))
                .thenReturn(mockedResponseForAppendMono);

        // Action
        Mono<Response<AppendBlobItem>> appendAsyncResponse = azureBlobStorageManager
                .appendAsync(blobName, data);

        /*
        Default values:
        Retries: 3
        Backoff ms: 4000 (4s)
        Max backoff ms: 120000 (120s)
        Connection timeout ms: 30000 (30s)
         */
        StepVerifier.withVirtualTime(() -> appendAsyncResponse)
                .thenAwait(Duration.ofSeconds(120)) // Waiting up to max backoff
                .expectNext(mockedResponseForAppend) // Expecting response after append request
                .verifyComplete();

        // Total number of performed retries
        assertEquals(3, failedResponseCount.get()); // 1 try and 2 retries

        // Verifying SDK calls
        verify(appendBlobAsyncClient, times(1))
                .createIfNotExistsWithResponse(any());

        verify(appendBlobAsyncClient, times(1))
                .appendBlockWithResponse(any(), anyLong(), any(), any());
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#commitBlockIdsAsync(String, List, boolean)} </b>.<br>
     * <b>Assumption:</b><br>
     * <ul>
     *     <li>Blob name is valid</li>
     *     <li>List of valid base64 encoded block ids</li>
     *     <li>Overwrite flag is false</li>
     * </ul>
     *
     * <p><b>Expectations:</b><br>
     * <ul>
     *     <li>{@link AzureBlobStorageManager#commitBlockIdsAsync(String, List, boolean)} should<br>
     *     return mono which when subscribed should return complete signal</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name, list of block ids and overwrite flag, "
            + "returned mono should return complete signal when subscribed")
    void commitBlockIdsAsync_givenBlobNameAndListOfBlockIdsAndOverwriteFlag_monoShouldReturnCompleteSignal() {

        // Testing with default retry values
        azureBlobStorageManager.configure(new HashMap<>());
        List<String> listOfBlockIds = getListOfBase64BlockIds();

        String blobName = "test-blob";

        BlockBlobItem mockBlockBlobItem = mock(BlockBlobItem.class);
        Mono<BlockBlobItem> mockResponseForCommitBlocks = Mono.just(mockBlockBlobItem);

        when(blockBlobAsyncClient.commitBlockList(any(), anyBoolean()))
                .thenReturn(mockResponseForCommitBlocks);

        // Action
        Mono<BlockBlobItem> responseForCommitBlocks = azureBlobStorageManager
                .commitBlockIdsAsync(blobName, listOfBlockIds, false);

        /*
        Default values:
        Retries: 3
        Backoff ms: 4000 (4s)
        Max backoff ms: 120000 (120s)
        Connection timeout ms: 30000 (30s)
         */
        StepVerifier
                .create(responseForCommitBlocks)
                .expectNext(mockBlockBlobItem)
                .verifyComplete();

        verify(blockBlobAsyncClient, times(1))
                .commitBlockList(any(), anyBoolean());
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#commitBlockIdsAsync(String, List, boolean)} </b>.<br>
     * <b>Assumption:</b><br>
     * <ul>
     *     <li>Blob name is valid</li>
     *     <li>List of valid base64 encoded block ids</li>
     *     <li>Overwrite flag is false</li>
     *     <li>All 3 retries should fail</li>
     * </ul>
     *
     * <p><b>Expectations:</b><br>
     * <ul>
     *     <li>Should retry 3 times (default config)</li>
     *     <li>{@link AzureBlobStorageManager#commitBlockIdsAsync(String, List, boolean)} should
     *     return mono which returns error</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name, list of block ids and overwrite flag, "
            + " when request fails returned mono should retry 3 times and return error")
    void commitBlockIdsAsync_givenBlobNameAndListOfBlockIdsAndOverwriteFlag_shouldRetry3Times_andReturnError() {

        // Testing with default retry values
        azureBlobStorageManager.configure(new HashMap<>());
        List<String> listOfBlockIds = getListOfBase64BlockIds();

        String blobName = "test-blob";
        AtomicInteger failedResponseCount = new AtomicInteger(0);

        /*
        Mocked create method to return mono of error
         */
        HttpResponse mockedHttpResponse = getMockHttpResponse();
        Mono<BlockBlobItem> mockedErrorResponseForCommitBlockMono = Mono.error(
                new com.azure.storage.blob.models.BlobStorageException("mocked", mockedHttpResponse, null));

        Mono<BlockBlobItem> retryCountResponseForCommitBlocks = mockedErrorResponseForCommitBlockMono
                .doOnError(com.azure.storage.blob.models.BlobStorageException.class,
                        t -> failedResponseCount.getAndIncrement());

        when(blockBlobAsyncClient.commitBlockList(any(), anyBoolean()))
                .thenReturn(retryCountResponseForCommitBlocks);

        // Action
        Mono<BlockBlobItem> responseForCommitBlocks = azureBlobStorageManager
                .commitBlockIdsAsync(blobName, listOfBlockIds, false);

        /*
        Default values:
        Retries: 3
        Backoff ms: 4000 (4s)
        Max backoff ms: 120000 (120s)
        Connection timeout ms: 30000 (30s)
         */
        StepVerifier.withVirtualTime(() -> responseForCommitBlocks)
                .thenAwait(Duration.ofSeconds(120)) // Waiting up to max backoff
                .expectError(BlobStorageException.class)
                .verify();

        assertEquals(4, failedResponseCount.get()); // 1 try and 3 retries

        // Asserting method invocation
        verify(blockBlobAsyncClient, times(1))
                .commitBlockList(any(), anyBoolean());
    }

    /**
     * <b>Method: {@link AzureBlobStorageManager#commitBlockIdsAsync(String, List, boolean)}</b>.<br>
     * <b>Assumption:</b><br>
     * <ul>
     *     <li>Blob name is valid</li>
     *     <li>List of valid base64 encoded block ids</li>
     *     <li>Overwrite flag is false</li>
     *     <li>returns mono of error for 1st try and 1 retries</li>
     *     <li>2nd retry is successful</li>
     * </ul>
     *
     * <p><b>Expectations:</b><br>
     * <ul>
     *     <li>Should retry until successful or max 3 (default)</li>
     *     <li>{@link AzureBlobStorageManager#commitBlockIdsAsync(String, List, boolean)} should
     *     return complete signal</li>
     * </ul>
     */
    @Test
    @DisplayName("Given blob name and block ids and overwrite flag, "
            + "assuming request failed for 1st try and 1 retries, "
            + "and 2nd retry is successful, should return complete signal when subscribed")
    void commitBlockIdsAsync_givenBlobNameAndBlockIdsAndOverwriteFlag_when2ndRetryIsSuccessful_shouldReturnComplete() {

        // Testing with default retry values
        azureBlobStorageManager.configure(new HashMap<>());
        List<String> listOfBlockIds = getListOfBase64BlockIds();

        String blobName = "test-blob";
        AtomicInteger failedResponseCount = new AtomicInteger(0);

        BlockBlobItem mockedResponseForCommitBlock = mock(BlockBlobItem.class);
        Mono<BlockBlobItem> mockedResponseForCommitBlockMono = Mono.just(mockedResponseForCommitBlock);

        /*
        Mocked create method to return mono of error
         */
        HttpResponse mockedHttpResponse = getMockHttpResponse();
        Mono<BlockBlobItem> mockMonoError = Mono.error(
                new com.azure.storage.blob.models.BlobStorageException("mocked", mockedHttpResponse, null));

        // Increment the counter for all failed requests (includes tries)
        // Check for the number of retries. If it is 2 (i.e 1 try and 1 retries), it should return
        // correct response.

        Mono<BlockBlobItem> retryCountMockedMono = mockedResponseForCommitBlockMono
                .flatMap(response -> {
                    if (failedResponseCount.get() != 2) {
                        return mockMonoError;
                    }
                    return mockedResponseForCommitBlockMono;
                })
                .doOnError(com.azure.storage.blob.models.BlobStorageException.class,
                        t -> failedResponseCount.getAndIncrement());

        // Mocking sdk calls
        when(blockBlobAsyncClient.commitBlockList(any(), anyBoolean()))
                .thenReturn(retryCountMockedMono);

        // Action
        Mono<BlockBlobItem> commitBlocksResponse = azureBlobStorageManager
                .commitBlockIdsAsync(blobName, listOfBlockIds, false);

        /*
        Default values:
        Retries: 3
        Backoff ms: 4000 (4s)
        Max backoff ms: 120000 (120s)
        Connection timeout ms: 30000 (30s)
         */
        StepVerifier.withVirtualTime(() -> commitBlocksResponse)
                .thenAwait(Duration.ofSeconds(120)) // Waiting up to max backoff
                .expectNext(mockedResponseForCommitBlock) // Expecting response after append request
                .verifyComplete();

        // Total number of performed retries
        assertEquals(2, failedResponseCount.get()); // 1 try and 1 retries

        // Verifying SDK calls
        verify(blockBlobAsyncClient, times(1))
                .commitBlockList(any(), anyBoolean());
    }

    private HttpResponse getMockHttpResponse() {
        HttpRequest mockHttpRequest = getMockHttpRequest();

        return new HttpResponse(mockHttpRequest) {
            @Override
            public int getStatusCode() {
                return 0;
            }

            @Override
            public String getHeaderValue(String s) {
                return null;
            }

            @Override
            public HttpHeaders getHeaders() {
                return null;
            }

            @Override
            public Flux<ByteBuffer> getBody() {
                return null;
            }

            @Override
            public Mono<byte[]> getBodyAsByteArray() {
                return null;
            }

            @Override
            public Mono<String> getBodyAsString() {
                return null;
            }

            @Override
            public Mono<String> getBodyAsString(Charset charset) {
                return null;
            }
        };
    }

    private HttpRequest getMockHttpRequest() {
        return new HttpRequest(HttpMethod.GET, "https://some.url.com");
    }

    private Response<AppendBlobItem> getMockResponse(int statusCode) {
        AppendBlobItem mockAppendBlobItem = new AppendBlobItem(
                "test-etag",
                OffsetDateTime.now(),
                null,
                false,
                "",
                "123",
                100
        );

        return new Response<>() {
            @Override
            public int getStatusCode() {
                return statusCode;
            }

            @Override
            public HttpHeaders getHeaders() {
                return null;
            }

            @Override
            public HttpRequest getRequest() {
                return null;
            }

            @Override
            public AppendBlobItem getValue() {
                return mockAppendBlobItem;
            }
        };
    }

    private Response<Void> getMockVoidResponse(int statusCode) {
        return new Response<>() {
            @Override
            public int getStatusCode() {
                return statusCode;
            }

            @Override
            public HttpHeaders getHeaders() {
                return null;
            }

            @Override
            public HttpRequest getRequest() {
                return null;
            }

            @Override
            public Void getValue() {
                return null;
            }
        };
    }

    private List<String> getListOfBase64BlockIds() {
        Base64.Encoder encoder = Base64.getEncoder();

        return new LinkedList<>() {
            {
                add(encoder
                        .encodeToString(UUID.randomUUID()
                                .toString()
                                .getBytes(StandardCharsets.UTF_8)
                        )
                );
                add(encoder
                        .encodeToString(UUID.randomUUID()
                                .toString()
                                .getBytes(StandardCharsets.UTF_8)
                        )
                );
            }
        };
    }
}
