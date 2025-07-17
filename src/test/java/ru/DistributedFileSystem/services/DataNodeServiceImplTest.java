package ru.DistributedFileSystem.services;

import com.example.grpc.DatanodeServiceOuterClass;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DataNodeServiceImplTest {

    DataNodeServiceImpl dataNodeService;

    @Mock
    StreamObserver<DatanodeServiceOuterClass.SuccessStatus> successStatusObserver;
    @Mock
    private StreamObserver<DatanodeServiceOuterClass.Chunks> chunksObserver;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        dataNodeService = new DataNodeServiceImpl();
    }

    @Test
    void writeFile_shouldStoreFileDataWithCorrectLoadId() {
        long loadId = 155L;
        byte[] testData = "test data".getBytes();

        StreamObserver<DatanodeServiceOuterClass.Chunks> requestObserver =
                dataNodeService.writeFile(successStatusObserver);

        requestObserver.onNext(DatanodeServiceOuterClass.Chunks.newBuilder()
                .setMetaData(DatanodeServiceOuterClass.FileData.newBuilder()
                        .setLoadId(loadId)
                        .build())
                .build());

        requestObserver.onNext(DatanodeServiceOuterClass.Chunks.newBuilder()
                .setChunks(com.google.protobuf.ByteString.copyFrom(testData))
                .build());

        requestObserver.onCompleted();

        verify(successStatusObserver).onNext(argThat(response ->
                response.getSuccess()
        ));
        verify(successStatusObserver).onCompleted();
        assertArrayEquals(testData, dataNodeService.datanode.get(loadId));
    }

    @Test
    void readFile_shouldReturnFileDataForLoadId() {
        long loadId = 592L;
        byte[] testData = "read test".getBytes();
        dataNodeService.datanode.put(loadId, testData);

        dataNodeService.readFile(DatanodeServiceOuterClass.FileData.newBuilder()
                .setLoadId(loadId)
                .build(), chunksObserver);

        verify(chunksObserver).onNext(argThat(chunk ->
                chunk.getChunks().toByteArray().length > 0
        ));
        verify(chunksObserver).onCompleted();
    }

    @Test
    void readFile_shouldFailForNonLoadId() {
        dataNodeService.readFile(DatanodeServiceOuterClass.FileData.newBuilder()
                .setLoadId(999L)
                .build(), chunksObserver);

        verify(chunksObserver).onError(argThat(throwable ->
                throwable instanceof io.grpc.StatusRuntimeException &&
                        ((io.grpc.StatusRuntimeException) throwable).getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND
        ));
    }
}