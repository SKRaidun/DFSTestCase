package ru.DistributedFileSystem.services;

import com.example.grpc.CoordinatorServiceGrpc;
import com.example.grpc.CoordinatorServiceOuterClass;
import io.grpc.stub.StreamObserver;
import ru.DistributedFileSystem.data.FileMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

public class CoordinatorServiceImpl extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {

    public final HashMap<String, FileMetaData> coordinator = new HashMap<>();
    public final ArrayList<Boolean> datanodesList =  new ArrayList<>(Arrays.asList(true, true, true));

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
            } else {
                responseBuilder.setNodeId(nodeId).setLoadId(loadId);
            }
        }

        responseStreamObserver.onNext(responseBuilder.build());
        responseStreamObserver.onCompleted();

    }


}
