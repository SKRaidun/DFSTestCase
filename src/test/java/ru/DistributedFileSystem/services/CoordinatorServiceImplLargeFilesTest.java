package ru.DistributedFileSystem.services;

import com.example.grpc.CoordinatorServiceOuterClass;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

//public class CoordinatorServiceImplLargeFilesTest {
//    private static final int CHUNK_SIZE = 1024 * 1024;
//    private static final String CHAR =
//            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 .,!?\n";
//
//    @Test
//    void writeManyFiles_shouldHandleMultipleRequests() {
//        CoordinatorServiceImplTest test = new CoordinatorServiceImplTest();
//        test.setUp();
//
//        int fileCount = 10000;
//        AtomicInteger successCount = new AtomicInteger();
//
//        for (int i = 0; i < fileCount; i++) {
//            String filePath = "/file_" + i + ".txt";
//            CoordinatorServiceOuterClass.Request request = CoordinatorServiceOuterClass.Request.newBuilder()
//                    .setFilePath(filePath)
//                    .build();
//
//            test.coordinatorService.writeFile(request, test.writeResponseObserver);
//
//            verify(test.writeResponseObserver, atLeastOnce()).onNext(any());
//            verify(test.writeResponseObserver, atLeastOnce()).onCompleted();
//
//            if (test.coordinatorService.coordinator.containsKey(filePath)) {
//                successCount.incrementAndGet();
//            }
//
//            reset(test.writeResponseObserver);
//        }
//
//        assertEquals(fileCount, successCount.get());
//    }
//
//    @Test
//    void writeLargeFiles_shouldDistributeAcrossNodes() {
//        CoordinatorServiceImplTest test = new CoordinatorServiceImplTest();
//        test.setUp();
//
//        int fileCount = 1000;
//        Map<Integer, Integer> nodeDistribution = new HashMap<>();
//
//        for (int i = 0; i < fileCount; i++) {
//            String filePath = "/large_file_" + i + ".txt";
//            CoordinatorServiceOuterClass.Request request = CoordinatorServiceOuterClass.Request.newBuilder()
//                    .setFilePath(filePath)
//                    .build();
//
//            test.coordinatorService.writeFile(request, test.writeResponseObserver);
//
//            ArgumentCaptor<CoordinatorServiceOuterClass.WriteResponse> captor =
//                    ArgumentCaptor.forClass(CoordinatorServiceOuterClass.WriteResponse.class);
//            verify(test.writeResponseObserver).onNext(captor.capture());
//
//            int nodeId = captor.getValue().getNodeId();
//            nodeDistribution.put(nodeId, nodeDistribution.getOrDefault(nodeId, 0) + 1);
//
//            reset(test.writeResponseObserver);
//        }
//
//        assertTrue(nodeDistribution.size() > 1, "Files should be distributed across nodes");
//    }
//
//    public static byte[] generateLargeTextFile(int sizeMB) {
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        Random random = new Random();
//        StringBuilder sb = new StringBuilder(CHUNK_SIZE * 2);
//
//        try {
//            int totalBytes = 0;
//            int targetBytes = sizeMB * 1024 * 1024;
//
//            while (totalBytes < targetBytes) {
//                sb.setLength(0);
//                while (sb.length() < CHUNK_SIZE && totalBytes < targetBytes) {
//                    char c = CHAR.charAt(random.nextInt(CHAR.length()));
//                    sb.append(c);
//                    totalBytes += Character.BYTES;
//                }
//                bos.write(sb.toString().getBytes(StandardCharsets.UTF_8));
//            }
//        } catch (IOException e) {
//            throw new RuntimeException("File generation failed", e);
//        }
//
//        return bos.toByteArray();
//    }
//}
