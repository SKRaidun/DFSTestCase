package ru.DistributedFileSystem.services;

import com.example.grpc.DatanodeServiceGrpc;
import com.example.grpc.DatanodeServiceOuterClass;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.DistributedFileSystem.data.FileMetaData;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class CoordinatorDeleteFilesTest {

    private Server dataNodeServer;
    private ManagedChannel dataNodeChannel;
    private String dataNodeServerName;

    private final List<Long> deleteCalls = Collections.synchronizedList(new ArrayList<>());

    private CoordinatorServiceImpl coordinator;

    private Map<String, FileMetaData> coordinatorMap;
    private Map<Long, byte[]> dataNodeStorage;

    @BeforeEach
    void setUp() throws Exception {
        deleteCalls.clear();

        BindableService mockDataNodeService = new DatanodeServiceGrpc.DatanodeServiceImplBase() {
            @Override
            public void deleteFile(DatanodeServiceOuterClass.toDeleteFile request,
                                   StreamObserver<DatanodeServiceOuterClass.SuccessStatus> responseObserver) {
                deleteCalls.add(request.getLoadId());
                responseObserver.onNext(DatanodeServiceOuterClass.SuccessStatus.newBuilder().setSuccess(true).build());
                responseObserver.onCompleted();
            }
        };

        dataNodeServerName = InProcessServerBuilder.generateName();
        dataNodeServer = InProcessServerBuilder
                .forName(dataNodeServerName)
                .directExecutor()
                .addService(mockDataNodeService)
                .build()
                .start();

        dataNodeChannel = InProcessChannelBuilder
                .forName(dataNodeServerName)
                .directExecutor()
                .build();

        coordinatorMap = new HashMap<>();
        dataNodeStorage = new HashMap<>();

        coordinator = new CoordinatorServiceImpl() {
            @Override
            public void deleteFileFromDataNode(int nodeId, long loadId) {
                dataNodeStorage.remove(loadId);
                DatanodeServiceGrpc.DatanodeServiceBlockingStub stub = DatanodeServiceGrpc.newBlockingStub(dataNodeChannel);
                stub.deleteFile(DatanodeServiceOuterClass.toDeleteFile.newBuilder().setLoadId(loadId).build());
            }
        };
        coordinator.coordinator = (HashMap<String, FileMetaData>) coordinatorMap;
    }

    @AfterEach
    void tearDown() throws Exception {
        if (dataNodeChannel != null) {
            dataNodeChannel.shutdownNow();
            dataNodeChannel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (dataNodeServer != null) {
            dataNodeServer.shutdownNow();
            dataNodeServer.awaitTermination();
        }
    }


    @Test
    void deleteFiles_shouldRemoveExpiredFinalizedFile_andCallDataNode() {
        String filePath = "/expired.txt";
        long loadId = 1L;
        int nodeId = 0;

        FileMetaData meta = new FileMetaData(nodeId, loadId);
        meta.setFinalize(true);
        meta.setExpires_at(Timestamp.valueOf(LocalDateTime.now().minusSeconds(10)));

        coordinatorMap.put(filePath, meta);
        dataNodeStorage.put(loadId, "data".getBytes(StandardCharsets.UTF_8));

        coordinator.deleteFiles().run();

        assertFalse("Файл должен быть удалён из coordinatorMap.", coordinatorMap.containsKey(filePath));
        assertFalse("Данные должны быть удалены из dataNodeStorage.", dataNodeStorage.containsKey(loadId));
        assertTrue("DataNode.deleteFile должен быть вызван.", deleteCalls.contains(loadId));
    }


    @Test
    void deleteFiles_shouldNotRemoveNonExpiredFile() {
        String filePath = "/valid.txt";
        long loadId = 2L;
        int nodeId = 0;

        FileMetaData meta = new FileMetaData(nodeId, loadId);
        meta.setFinalize(true);
        meta.setExpires_at(Timestamp.valueOf(LocalDateTime.now().plusSeconds(60)));

        coordinatorMap.put(filePath, meta);
        dataNodeStorage.put(loadId, "data".getBytes(StandardCharsets.UTF_8));

        coordinator.deleteFiles().run();

        assertTrue("Файл не должен быть удалён.", coordinatorMap.containsKey(filePath));
        assertTrue("Данные должны остаться.", dataNodeStorage.containsKey(loadId));
        assertFalse("DataNode.deleteFile не должен вызываться.", deleteCalls.contains(loadId));
    }

    @Test
    void deleteFiles_mixedSet() {
        addMeta("/A.txt", 10L, true, -5);
        addMeta("/B.txt", 11L, true, +60);
        addMeta("/C.txt", 12L, false, -5);

        coordinator.deleteFiles().run();

        assertFalse(coordinatorMap.containsKey("/A.txt"));
        assertTrue(coordinatorMap.containsKey("/B.txt"));
        assertFalse(coordinatorMap.containsKey("/C.txt"));

        assertFalse(dataNodeStorage.containsKey(10L));
        assertTrue(dataNodeStorage.containsKey(11L));
        assertFalse(dataNodeStorage.containsKey(12L));
    }

    private void addMeta(String filePath, long loadId, boolean fin, int expireShiftSeconds) {
        FileMetaData meta = new FileMetaData(0, loadId);
        meta.setFinalize(fin);
        meta.setExpires_at(Timestamp.valueOf(LocalDateTime.now().plusSeconds(expireShiftSeconds)));
        coordinatorMap.put(filePath, meta);
        dataNodeStorage.put(loadId, ("data"+loadId).getBytes(StandardCharsets.UTF_8));
    }
}
