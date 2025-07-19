package ru.DistributedFileSystem.controllers;

import com.example.grpc.CoordinatorServiceGrpc;
import com.example.grpc.CoordinatorServiceOuterClass;
import com.example.grpc.DatanodeServiceGrpc;
import com.example.grpc.DatanodeServiceOuterClass;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClientController {

    public CoordinatorServiceGrpc.CoordinatorServiceBlockingStub coordinatorServiceGrpc;

    public ClientController(Channel channel) {
        this.coordinatorServiceGrpc = CoordinatorServiceGrpc.newBlockingStub(channel);
    }



    public void writeFileRequest(String filePath, byte[] data) {
        CoordinatorServiceOuterClass.Request request = CoordinatorServiceOuterClass.Request.newBuilder().setFilePath(filePath).build();
        CoordinatorServiceOuterClass.WriteResponse response = coordinatorServiceGrpc.writeFile(request);


        CountDownLatch finishLatch = new CountDownLatch(1);

        ManagedChannel dataNodeChannel = ManagedChannelBuilder.forAddress("localhost", 50051 + response.getNodeId())
                .usePlaintext()
                .maxInboundMessageSize(100 * 1024 * 1024)
                .build();

        DatanodeServiceGrpc.DatanodeServiceStub datanodeServiceStub = DatanodeServiceGrpc.newStub(dataNodeChannel);


        int nodeId = response.getNodeId();
        Long loadId = response.getLoadId();



        StreamObserver<DatanodeServiceOuterClass.SuccessStatus> streamObserver = new StreamObserver<DatanodeServiceOuterClass.SuccessStatus>() {
            @Override
            public void onNext(DatanodeServiceOuterClass.SuccessStatus successStatus) {

            }

            @Override
            public void onError(Throwable throwable) {
                finishLatch.countDown();
                throw new RuntimeException("Upload Error");
            }

            @Override
            public void onCompleted() {
                finishLatch.countDown();
            }
        };


        StreamObserver<DatanodeServiceOuterClass.Chunks> uploadStream = datanodeServiceStub.writeFile(streamObserver);
        uploadStream.onNext(DatanodeServiceOuterClass.Chunks.newBuilder().setMetaData(DatanodeServiceOuterClass.FileData.newBuilder().setLoadId(loadId).setNodeId(nodeId).build()).build());

        int chunkSize = 1024 * 1024;
        for (int i = 0; i < data.length; i += chunkSize) {
            int end = Math.min(i + chunkSize, data.length);
            uploadStream.onNext(DatanodeServiceOuterClass.Chunks.newBuilder().setChunks(ByteString.copyFrom(data, i, end - i)).build());
        }

        CoordinatorServiceOuterClass.expiresTime expiresTime = CoordinatorServiceOuterClass.expiresTime.newBuilder().setFilePath(filePath).build();
        coordinatorServiceGrpc.setExpiresAtTime(expiresTime);
        uploadStream.onCompleted();

        try {
            if (!finishLatch.await(60, TimeUnit.SECONDS)) {
                throw new RuntimeException("Upload timeout");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Upload interrupted", e);
        } finally {
            dataNodeChannel.shutdownNow();
        }
    }


    public byte[] readFileRequest(String filePath) throws IOException {
        CoordinatorServiceOuterClass.ReadResponse response = coordinatorServiceGrpc.readFile(
                CoordinatorServiceOuterClass.Request.newBuilder().setFilePath(filePath).build()
        );


        ManagedChannel dataNodeChannel = ManagedChannelBuilder.forAddress("localhost", 50051 + response.getNodeId())
                .usePlaintext()
                .maxInboundMessageSize(500 * 1024 * 1024)
                .build();

        try {
            DatanodeServiceGrpc.DatanodeServiceBlockingStub blockingStub =
                    DatanodeServiceGrpc.newBlockingStub(dataNodeChannel);

            Iterator<DatanodeServiceOuterClass.Chunks> chunks = blockingStub.readFile(
                    DatanodeServiceOuterClass.FileData.newBuilder()
                            .setLoadId(response.getLoadId())
                            .build()
            );

            ByteArrayOutputStream fileData = new ByteArrayOutputStream();
            while (chunks.hasNext()) {
                fileData.write(chunks.next().getChunks().toByteArray());
            }

            CoordinatorServiceOuterClass.expiresTime expiresTime = CoordinatorServiceOuterClass.expiresTime.newBuilder().setFilePath(filePath).build();
            coordinatorServiceGrpc.setExpiresAtTime(expiresTime);

            return fileData.toByteArray();
        } finally {
            dataNodeChannel.shutdownNow();
        }
    }

}
