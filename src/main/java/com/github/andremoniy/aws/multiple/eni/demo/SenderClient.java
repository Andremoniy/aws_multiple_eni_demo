package com.github.andremoniy.aws.multiple.eni.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.File;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static com.github.andremoniy.aws.multiple.eni.demo.SenderDaemon.getLastChunkNumber;

public class SenderClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderClient.class);

    public static void main(String[] args) {
        // 1. file path

        // ToDo: usage

        final String filePath = args[0];
        final File file = new File(filePath);

        try (var bufferedInputStream = new BufferedInputStream(
                new FileInputStream(file),
                SenderDaemon.BLOCK_SIZE
        )) {
            final long startTime = System.nanoTime();
            try (Socket socket = new Socket("192.168.178.22", SenderDaemon.PORT)) {
                try (var dataOutputStream = new DataOutputStream(socket.getOutputStream());
                     var dataInputStream = new DataInputStream(socket.getInputStream())) {

                    dataOutputStream.writeLong(0);
                    dataOutputStream.writeLong(file.length());
                    dataOutputStream.writeUTF(file.getName());
                    final long transactionId = dataInputStream.readLong();

                    final int lastChunkNumber = getLastChunkNumber(file.length());
                    for (int chunkNumber = 1; chunkNumber <= lastChunkNumber; chunkNumber++) {

                        byte[] block;
                        if (chunkNumber == lastChunkNumber) {
                            block = new byte[SenderDaemon.getLastChunkSize(file.length(), lastChunkNumber)];
                        } else {
                            block = new byte[SenderDaemon.BLOCK_SIZE];
                        }
                        bufferedInputStream.read(block);
                        final byte[] blockToWrite;
                        if (chunkNumber == lastChunkNumber) {
                            blockToWrite = new byte[SenderDaemon.BLOCK_SIZE];
                            System.arraycopy(block, 0, blockToWrite, 0, block.length);
                        } else {
                            blockToWrite = block;
                        }

                        dataOutputStream.writeLong(transactionId);
                        dataOutputStream.writeInt(chunkNumber);
                        dataOutputStream.write(blockToWrite);
                    }
                }
            }
            final long uploadingTimeInSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
            LOGGER.info("Uploading time: {}", uploadingTimeInSeconds);
            final double speed = (double) file.length() * 8 / uploadingTimeInSeconds;
            LOGGER.info("Throughput: {} Bs", speed);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
