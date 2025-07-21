package ru.DistributedFileSystem;

import io.grpc.*;
import ru.DistributedFileSystem.controllers.ClientController;
import ru.DistributedFileSystem.services.CoordinatorServiceImpl;
import ru.DistributedFileSystem.services.DataNodeServiceImpl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;



public class App 
{
    private static final int CHUNK_SIZE = 1024 * 1024;
    private static final String CHAR =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 .,!?\n";

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



    public static void main( String[] args ) throws IOException, InterruptedException {

        Server server = ServerBuilder.forPort(8080).addService(new CoordinatorServiceImpl()).build();
        server.start();



        int[] dataNodePorts = {50051, 50052, 50053};
        for (int port : dataNodePorts) {
            Server dataNode = ServerBuilder.forPort(port)
                    .addService(new DataNodeServiceImpl())
                    .build()
                    .start();
        }

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                        .usePlaintext().build();

        ClientController client = new ClientController(channel);

        for (int i = 0; i < 3; i++) {
            byte[] fileContent = generateLargeTextFile(1);
            client.writeFileRequest("/file_test_" + i, fileContent);

            if (i == 1) {
                Thread.sleep(5000);
                continue;
            }
            if (i == 2) {
               Thread.sleep(5000);
            }
        }

        for (int i = 0; i < 3; i++) {
            String filePath = "/file_test_" + i;
            try {
                byte[] content = client.readFileRequest(filePath);
                System.out.println("Файл " + filePath + " существует, размер: " + content.length + " байт");
            } catch (Exception e) {
                System.out.println("Файл " + filePath + " удалён (ожидаемо для i=1 и i=2): " + e.getMessage());
            }
        }

        Thread.sleep(20000);


        for (int i = 0; i < 3; i++) {
            String filePath = "/file_test_" + i;
            try {
                byte[] content = client.readFileRequest(filePath);
                System.out.println("Файл " + filePath + " существует, размер: " + content.length + " байт");
            } catch (Exception e) {
                System.out.println("Файл " + filePath + " удалён (ожидаемо для i=1 и i=2): " + e.getMessage());
            }
        }
    }
}
