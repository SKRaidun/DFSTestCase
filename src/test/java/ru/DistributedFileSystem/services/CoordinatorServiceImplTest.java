package ru.DistributedFileSystem.services;

import com.example.grpc.CoordinatorServiceOuterClass;
import com.example.grpc.DatanodeServiceGrpc;
import com.example.grpc.DatanodeServiceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static junit.framework.Assert.assertEquals;
import ru.DistributedFileSystem.data.FileMetaData;

import java.io.IOException;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CoordinatorServiceImplTest {

    CoordinatorServiceImpl coordinatorService;
    StreamObserver<CoordinatorServiceOuterClass.WriteResponse> writeResponseObserver;
    private StreamObserver<CoordinatorServiceOuterClass.ReadResponse> readResponseObserver;

    @BeforeEach
    void setUp() {
        coordinatorService = new CoordinatorServiceImpl();
        writeResponseObserver = mock(StreamObserver.class);
        readResponseObserver = mock(StreamObserver.class);
    }

    @Test
    void writeFile_shouldAssignNewFile() {
        CoordinatorServiceOuterClass.Request request = CoordinatorServiceOuterClass.Request.newBuilder()
                .setFilePath("/I_WANT_TO_WORK.txt")
                .build();

        coordinatorService.writeFile(request, writeResponseObserver);

        verify(writeResponseObserver).onNext(any(CoordinatorServiceOuterClass.WriteResponse.class));
        verify(writeResponseObserver).onCompleted();
    }

    @Test
    void writeFile_shouldReturnErrorForExistingFile() {
        String filePath = "/existssss.txt";
        coordinatorService.coordinator.put(filePath, new FileMetaData(0, 159L));

        CoordinatorServiceOuterClass.Request request = CoordinatorServiceOuterClass.Request.newBuilder()
                .setFilePath(filePath)
                .build();

        coordinatorService.writeFile(request, writeResponseObserver);

        verify(writeResponseObserver).onNext(argThat(response ->
                response.getErrorCode() == CoordinatorServiceOuterClass.WriteResponse.ErrorCode.ALREADY_EXISTS
        ));
        verify(writeResponseObserver).onCompleted();
    }

    @Test
    void writeFile_shouldAssignFileToAvailableNode() {
        CoordinatorServiceOuterClass.Request request = CoordinatorServiceOuterClass.Request.newBuilder()
                .setFilePath("/newnewnew.txt")
                .build();
        coordinatorService.datanodesList.set(1, false);
        coordinatorService.datanodesList.set(2, false);

        coordinatorService.writeFile(request, writeResponseObserver);

        verify(writeResponseObserver).onNext(argThat(response ->
                response.getNodeId() == 0
        ));

        verify(writeResponseObserver).onCompleted();
    }

    @Test
    void readFile_shouldReturnMetaDataForExistingFile() {
        String filePath = "/exist.txt";
        coordinatorService.coordinator.put(filePath, new FileMetaData(1, 912L));


        for (Map.Entry<String, FileMetaData> entry : coordinatorService.coordinator.entrySet()) {
            FileMetaData data = entry.getValue();
            System.out.println(data.getNodeId());
        }

        assertEquals(coordinatorService.coordinator.get(filePath).getNodeId(), 1);
        assertEquals(coordinatorService.coordinator.get(filePath).getLoadId(), 912L);

    }

    @Test
    void readFile_shouldReturnErrorForNonExistentFile() {
        CoordinatorServiceOuterClass.Request request = CoordinatorServiceOuterClass.Request.newBuilder()
                .setFilePath("/nonexistent.txt")
                .build();

        coordinatorService.readFile(request, readResponseObserver);

        verify(readResponseObserver).onNext(argThat(response ->
                response.getErrorCode() == CoordinatorServiceOuterClass.ReadResponse.ErrorCode.NOT_FOUND
        ));
        verify(readResponseObserver).onCompleted();
    }

    @Test
    void readFile_shouldReturnErrorWhenNodeIsUnavailable() {
        String filePath = "/unavailable.txt";
        coordinatorService.coordinator.put(filePath, new FileMetaData(1, 789L));
        coordinatorService.datanodesList.set(1, false);

        CoordinatorServiceOuterClass.Request request = CoordinatorServiceOuterClass.Request.newBuilder()
                .setFilePath(filePath)
                .build();

        coordinatorService.readFile(request, readResponseObserver);

        verify(readResponseObserver).onNext(argThat(response ->
                response.getErrorCode() == CoordinatorServiceOuterClass.ReadResponse.ErrorCode.RESOURCE_EXHAUSTED &&
                        response.getErrorMessage().equals("Datanode 1 is unavailable")
        ));
        verify(readResponseObserver).onCompleted();
    }

}