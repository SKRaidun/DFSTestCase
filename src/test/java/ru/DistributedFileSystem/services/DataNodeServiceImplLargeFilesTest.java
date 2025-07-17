package ru.DistributedFileSystem.services;

import com.example.grpc.DatanodeServiceOuterClass;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;

public class DataNodeServiceImplLargeFilesTest {
    private static final int CHUNK_SIZE = 1024 * 1024;
    private static final String CHAR =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 .,!?\n";

    @Test
    void writeManyFiles_shouldStoreMultipleFiles() {
        DataNodeServiceImplTest test = new DataNodeServiceImplTest();
        test.setUp();

        int fileCount = 10000;
        AtomicInteger successCount = new AtomicInteger();

        for (long i = 0; i < fileCount; i++) {
            byte[] testData = ("Data " + i).getBytes();
            StreamObserver<DatanodeServiceOuterClass.Chunks> requestObserver =
                    test.dataNodeService.writeFile(test.successStatusObserver);

            requestObserver.onNext(DatanodeServiceOuterClass.Chunks.newBuilder()
                    .setMetaData(DatanodeServiceOuterClass.FileData.newBuilder()
                            .setLoadId(i)
                            .build())
                    .build());

            requestObserver.onNext(DatanodeServiceOuterClass.Chunks.newBuilder()
                    .setChunks(com.google.protobuf.ByteString.copyFrom(testData))
                    .build());

            requestObserver.onCompleted();

            if (Arrays.equals(testData, test.dataNodeService.datanode.get(i))) {
                successCount.incrementAndGet();
            }
        }

        assertEquals(fileCount, successCount.get());
    }

    @Test
    void writeLargeFile_shouldHandleBigData() {
        DataNodeServiceImplTest test = new DataNodeServiceImplTest();
        test.setUp();

        long loadId = 99991L;
        byte[] largeData = generateLargeTextFile(1000);

        StreamObserver<DatanodeServiceOuterClass.Chunks> requestObserver =
                test.dataNodeService.writeFile(test.successStatusObserver);

        requestObserver.onNext(DatanodeServiceOuterClass.Chunks.newBuilder()
                .setMetaData(DatanodeServiceOuterClass.FileData.newBuilder()
                        .setLoadId(loadId)
                        .build())
                .build());

        requestObserver.onNext(DatanodeServiceOuterClass.Chunks.newBuilder()
                .setChunks(com.google.protobuf.ByteString.copyFrom(largeData))
                .build());

        requestObserver.onCompleted();

        verify(test.successStatusObserver).onNext(argThat(response ->
                response.getSuccess()));
        verify(test.successStatusObserver).onCompleted();
        assertArrayEquals(largeData, test.dataNodeService.datanode.get(loadId));
    }

    public static byte[] generateLargeTextFile(int sizeMB) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Random random = new Random();
        StringBuilder sb = new StringBuilder(CHUNK_SIZE * 2);

        try {
            int totalBytes = 0;
            int targetBytes = sizeMB * 1024 * 1024;

            while (totalBytes < targetBytes) {
                sb.setLength(0);
                while (sb.length() < CHUNK_SIZE && totalBytes < targetBytes) {
                    char c = CHAR.charAt(random.nextInt(CHAR.length()));
                    sb.append(c);
                    totalBytes += Character.BYTES;
                }
                bos.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new RuntimeException("File generation failed", e);
        }

        return bos.toByteArray();
    }
}
