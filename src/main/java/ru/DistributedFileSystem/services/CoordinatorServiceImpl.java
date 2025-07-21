package ru.DistributedFileSystem.services;

import com.example.grpc.CoordinatorServiceGrpc;
import com.example.grpc.CoordinatorServiceOuterClass;
import com.example.grpc.DatanodeServiceGrpc;
import com.example.grpc.DatanodeServiceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import ru.DistributedFileSystem.data.FileMetaData;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class CoordinatorServiceImpl extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {

    public HashMap<String, FileMetaData> coordinator = new HashMap<>();
    public final ArrayList<Boolean> datanodesList =  new ArrayList<>(Arrays.asList(true, true, true));
    public final ScheduledExecutorService fileDaemon;

    public CoordinatorServiceImpl() {
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        };
        this.fileDaemon = Executors.newSingleThreadScheduledExecutor(threadFactory);
        cleanOldFiles();
    }

    public void cleanOldFiles() {
        fileDaemon.scheduleAtFixedRate(deleteFiles(), 0, 10, TimeUnit.SECONDS);
    }


    Runnable deleteFiles() {

        return () -> {

            Timestamp currentTime = Timestamp.valueOf(LocalDateTime.now());

            Iterator<Map.Entry<String, FileMetaData>> iterator = coordinator.entrySet().iterator();

            while(iterator.hasNext()) {
                Map.Entry<String, FileMetaData> element = iterator.next();
                FileMetaData meta = element.getValue();
                if (meta.getExpires_at().compareTo(currentTime) < 0) {
                    deleteFileFromDataNode(meta.getNodeId(), meta.getLoadId());
                    iterator.remove();
                }
                }

            System.out.println("OPA");
        };
    }

    protected void deleteFileFromDataNode(int nodeId, long loadId) {

        ManagedChannel dataNodeChannel = ManagedChannelBuilder.forAddress("localhost", 50051 + nodeId)
                .usePlaintext()
                .build();

        DatanodeServiceGrpc.DatanodeServiceBlockingStub datanodeServiceBlockingStub = DatanodeServiceGrpc.newBlockingStub(dataNodeChannel);
        datanodeServiceBlockingStub.deleteFile(DatanodeServiceOuterClass.toDeleteFile.newBuilder().setLoadId(loadId).build());

    }

    @Override
    public void writeFile(CoordinatorServiceOuterClass.Request request, StreamObserver<CoordinatorServiceOuterClass.WriteResponse> responseStreamObserver) {

        Random random = new Random();

        String filePath = request.getFilePath();

        CoordinatorServiceOuterClass.WriteResponse.Builder responseBuilder= CoordinatorServiceOuterClass.WriteResponse.newBuilder();

        ArrayList<Integer> aliveNodes = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            if (datanodesList.get(i)) aliveNodes.add(i);
        }
        if (coordinator.containsKey(filePath)) {
            responseBuilder.setErrorCode(CoordinatorServiceOuterClass.WriteResponse.ErrorCode.ALREADY_EXISTS).setErrorMessage("File already exists");
        } else {
            int randomNumber = random.nextInt(aliveNodes.size());
            int randomNodeNumber = aliveNodes.get(randomNumber);
            long randomLoadId = new Random().nextLong();
            if (datanodesList.get(randomNodeNumber)) {
                coordinator.put(filePath, new FileMetaData(randomNodeNumber, randomLoadId));
            }
            responseBuilder.setNodeId(randomNodeNumber).setLoadId(randomLoadId);
        }


        responseStreamObserver.onNext(responseBuilder.build());
        responseStreamObserver.onCompleted();
    }

    @Override
    public void readFile(CoordinatorServiceOuterClass.Request request, StreamObserver<CoordinatorServiceOuterClass.ReadResponse> responseStreamObserver) {

        String filePath = request.getFilePath();

        CoordinatorServiceOuterClass.ReadResponse.Builder responseBuilder= CoordinatorServiceOuterClass.ReadResponse.newBuilder();


        if (!coordinator.containsKey(filePath)) {
            responseBuilder.setErrorCode(CoordinatorServiceOuterClass.ReadResponse.ErrorCode.NOT_FOUND)
                    .setErrorMessage("File not found");
        } else {
            FileMetaData metaData = coordinator.get(filePath);
            int nodeId = metaData.getNodeId();
            long loadId = metaData.getLoadId();

            if (!datanodesList.get(nodeId)) {
                responseBuilder.setErrorCode(CoordinatorServiceOuterClass.ReadResponse.ErrorCode.RESOURCE_EXHAUSTED)
                        .setErrorMessage("Datanode " + nodeId + " is unavailable");
            } else if (!metaData.isFinalize()) {
                responseBuilder.setErrorCode(CoordinatorServiceOuterClass.ReadResponse.ErrorCode.RESOURCE_EXHAUSTED)
                        .setErrorMessage("File " + loadId + " is unavailable");
            }
            else {
                responseBuilder.setNodeId(nodeId).setLoadId(loadId);
            }
        }

        responseStreamObserver.onNext(responseBuilder.build());
        responseStreamObserver.onCompleted();

    }

    @Override
    public void setExpiresAtTime(CoordinatorServiceOuterClass.expiresTime expiresAtTime, StreamObserver<CoordinatorServiceOuterClass.timeStatus> timeStatusStreamObserver) {
        String filePath = expiresAtTime.getFilePath();
        FileMetaData metaData = coordinator.get(filePath);
        metaData.setExpires_at(Timestamp.valueOf(LocalDateTime.now().plusSeconds(120)));
        metaData.setFinalize(true);
        coordinator.put(filePath, metaData);

        CoordinatorServiceOuterClass.timeStatus.Builder builder = CoordinatorServiceOuterClass.timeStatus.newBuilder();
        builder.setErrorCode(CoordinatorServiceOuterClass.timeStatus.ErrorCode.OK);
        timeStatusStreamObserver.onNext(builder.build());
        timeStatusStreamObserver.onCompleted();
    }


}
