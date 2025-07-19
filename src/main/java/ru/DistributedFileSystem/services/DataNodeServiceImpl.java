package ru.DistributedFileSystem.services;

import com.example.grpc.DatanodeServiceGrpc;
import com.example.grpc.DatanodeServiceOuterClass;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;

public class DataNodeServiceImpl extends DatanodeServiceGrpc.DatanodeServiceImplBase {
    final HashMap<Long, byte[]> datanode = new HashMap<>();



    @Override
    public StreamObserver<DatanodeServiceOuterClass.Chunks> writeFile(StreamObserver<DatanodeServiceOuterClass.SuccessStatus> streamObserver) {
        return new StreamObserver<DatanodeServiceOuterClass.Chunks>() {

            ByteArrayOutputStream data = new ByteArrayOutputStream();
            Long loadId = null;
            @Override
            public void onNext(DatanodeServiceOuterClass.Chunks chunks) {
                if (chunks.hasMetaData()) {
                    loadId = chunks.getMetaData().getLoadId();
                } else {
                    try {
                        data.write(chunks.getChunks().toByteArray());
                    } catch (IOException e) {
                        streamObserver.onError(Status.INTERNAL
                                .withDescription("Failed to write chunk")
                                .withCause(e)
                                .asRuntimeException());
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                streamObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                datanode.put(loadId, data.toByteArray());

                System.out.println(datanode.get(loadId));
                streamObserver.onNext(DatanodeServiceOuterClass.SuccessStatus.newBuilder().setSuccess(true).build());
                streamObserver.onCompleted();
            }
        };
    }

    @Override
    public void readFile(DatanodeServiceOuterClass.FileData fileData, StreamObserver<DatanodeServiceOuterClass.Chunks> streamObserver) {
        Long loadId = fileData.getLoadId();

        byte[] file = datanode.get(loadId);

        if (file == null) {
            streamObserver.onError(
                    Status.NOT_FOUND
                            .withDescription("Файл с loadId " + loadId + " не найден на DataNode")
                            .asRuntimeException()
            );
            return;
        }


        int chunkSize = 1024 * 1024;
        for (int i = 0; i < file.length; i+=chunkSize) {
            int end = Math.min(i + chunkSize, file.length) ;
            DatanodeServiceOuterClass.Chunks chunks = DatanodeServiceOuterClass.Chunks.newBuilder().setChunks(ByteString.copyFrom(file, i, end - i)).build();
            streamObserver.onNext(chunks);
        }

        streamObserver.onCompleted();
    }


}
