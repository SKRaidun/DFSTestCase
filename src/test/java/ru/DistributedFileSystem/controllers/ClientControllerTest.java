package ru.DistributedFileSystem.controllers;

import com.example.grpc.CoordinatorServiceGrpc;
import com.example.grpc.CoordinatorServiceOuterClass;
import com.example.grpc.DatanodeServiceGrpc;
import com.example.grpc.DatanodeServiceOuterClass;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ClientControllerTest {

    private ClientController clientController;
    private CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordinatorStub;
    private DatanodeServiceGrpc.DatanodeServiceStub datanodeStub;

    @BeforeEach
    void setUp() {
        Channel mockChannel = mock(Channel.class);
        coordinatorStub = mock(CoordinatorServiceGrpc.CoordinatorServiceBlockingStub.class);
        datanodeStub = mock(DatanodeServiceGrpc.DatanodeServiceStub.class);

        when(mockChannel.newCall(any(), any())).thenReturn(null);
        clientController = new ClientController(mockChannel);
        clientController.coordinatorServiceGrpc = coordinatorStub;
    }

    @Test
    void writeFileRequest_shouldSendDataToDataNode() {
        when(coordinatorStub.writeFile(any()))
                .thenReturn(CoordinatorServiceOuterClass.WriteResponse.newBuilder()
                        .setNodeId(1)
                        .setLoadId(229L)
                        .build());

        ManagedChannel mockManagedChannel = mock(ManagedChannel.class);
        when(mockManagedChannel.newCall(any(), any())).thenReturn(null);
        when(mockManagedChannel.isShutdown()).thenReturn(false);
        when(mockManagedChannel.isTerminated()).thenReturn(false);

        StreamObserver<DatanodeServiceOuterClass.SuccessStatus> mockStatusObserver = mock(StreamObserver.class);
        when(datanodeStub.writeFile(any())).thenReturn(mock(StreamObserver.class));

        byte[] testData = "/I_WANT_TO_WORK".getBytes();
        clientController.writeFileRequest("/I_WANT_TO_WORK", testData);

        verify(coordinatorStub).writeFile(any());
    }
}