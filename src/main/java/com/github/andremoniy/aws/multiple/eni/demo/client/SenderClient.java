package com.github.andremoniy.aws.multiple.eni.demo.client;

import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceNetworkInterface;
import com.amazonaws.services.ec2.model.Reservation;
import com.github.andremoniy.aws.multiple.eni.demo.util.SenderTools;
import com.github.andremoniy.aws.multiple.eni.demo.util.ArrayLoop;
import com.github.andremoniy.aws.multiple.eni.demo.data.DataChunk;
import com.github.andremoniy.aws.multiple.eni.demo.util.Loop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SenderClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderClient.class);
    private static final DataChunk END_OF_QUEUE = new DataChunk(0, 0, null);

    public static void main(String[] args) throws SocketException {
        if (args.length != 2) {
            System.out.println("Usage: SenderClient <File path> <IP address | EC2 instance ID>");
            System.exit(0);
        }

        final String filePath = args[0];
        final File file = new File(filePath);

        final Loop<String> hostsLoop;

        if (!args[1].contains(".")) {
            hostsLoop = getHostsLoopFromEC2Instance(args[1]);
        } else {
            hostsLoop = new ArrayLoop<>(List.of(args[1]));
        }

        final List<NetworkInterface> networkInterfaces = SenderTools.getNetworkInterfaces();
        LOGGER.info("Found {} local network interfaces: {}", networkInterfaces.size(), networkInterfaces);
        final ExecutorService executorService = Executors.newFixedThreadPool(networkInterfaces.size());
        final BlockingQueue<DataChunk> queue = new ArrayBlockingQueue<>(100);

        final long lastChunkNumber = SenderTools.getLastChunkNumber(file.length());

        for (NetworkInterface networkInterface : networkInterfaces) {
            final Optional<InetAddress> firstInet4Address = SenderTools.getInet4Address(networkInterface);

            if (firstInet4Address.isEmpty()) {
                LOGGER.info("There is no Inet4 address in the interface {}, skipping", networkInterface);
                continue;
            }

            final String nextHost = hostsLoop.getNext();
            LOGGER.info("Binding {} to {}", networkInterface, nextHost);
            executorService.submit(new SocketSender(queue, firstInet4Address.get(), nextHost, lastChunkNumber));
        }

        // The main thread sends a handshake

        LOGGER.info("Starting negotiation with the server...");
        try (var bufferedInputStream = new BufferedInputStream(
                new FileInputStream(file),
                SenderTools.BLOCK_SIZE
        )) {
            final long startTime = System.nanoTime();
            final long transactionId = handShakeAndGetTxId(file, hostsLoop);

            if (transactionId <= 0) {
                LOGGER.error("Received {} as a transaction id which means an error on server side", transactionId);
                return;
            }

            LOGGER.info("A handshake initialised, transactionId: {}", transactionId);

            LOGGER.info("File will be split on {} chunks", lastChunkNumber);
            for (long chunkNumber = 1; chunkNumber <= lastChunkNumber; chunkNumber++) {

                final byte[] block;
                if (chunkNumber == lastChunkNumber) {
                    block = new byte[SenderTools.getLastChunkSize(file.length(), lastChunkNumber)];
                } else {
                    block = new byte[SenderTools.BLOCK_SIZE];
                }
                bufferedInputStream.read(block);
                final byte[] blockToWrite;
                if (chunkNumber == lastChunkNumber) {
                    blockToWrite = new byte[SenderTools.BLOCK_SIZE];
                    System.arraycopy(block, 0, blockToWrite, 0, block.length);
                } else {
                    blockToWrite = block;
                }

                queue.put(new DataChunk(transactionId, chunkNumber, blockToWrite));
            }
            queue.put(END_OF_QUEUE);
            executorService.shutdown();
            while (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                LOGGER.info("Waiting for uploading threads termination");
            }

            final long uploadingDurationInNanos = System.nanoTime() - startTime;
            LOGGER.info("Uploading time: {} ns, {} s", uploadingDurationInNanos, TimeUnit.NANOSECONDS.toSeconds(uploadingDurationInNanos));
            final double speedMBs = ((double) file.length() * 8 / uploadingDurationInNanos) * TimeUnit.SECONDS.toNanos(1) / 1024 / 1024;
            final double speedGBs = speedMBs / 1024;
            LOGGER.info("Throughput: {} MBs, {} GBs", speedMBs, speedGBs);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private static long handShakeAndGetTxId(final File file, final Loop<String> hostsLoop) throws IOException {
        try (Socket socket = new Socket(hostsLoop.getNext(), SenderTools.PORT);
             var dataOutputStream = new DataOutputStream(socket.getOutputStream());
             var dataInputStream = new DataInputStream(socket.getInputStream())) {

            dataOutputStream.writeLong(0); // start a handshake
            dataOutputStream.writeLong(file.length());
            dataOutputStream.writeUTF(file.getName());
            dataOutputStream.flush();

            LOGGER.info("Waiting for transaction ID...");
            // read transaction id
            return dataInputStream.readLong();
        }
    }

    private static Loop<String> getHostsLoopFromEC2Instance(final String ec2InstanceId) {
        LOGGER.info("Fetching network interfaces for ec2 {}", ec2InstanceId);
        final DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest().withInstanceIds(ec2InstanceId);
        final DescribeInstancesResult describeInstancesResult = AmazonEC2ClientBuilder.defaultClient().describeInstances(describeInstancesRequest);
        final Reservation reservation = describeInstancesResult.getReservations().get(0);
        final Instance instance = reservation.getInstances().get(0);

        final List<String> hosts = instance.getNetworkInterfaces().stream()
                .map(InstanceNetworkInterface::getPrivateIpAddress)
                .collect(Collectors.toList());

        LOGGER.info("Found following ip addresses on remote EC2 {}:\n{}", ec2InstanceId, hosts);

        return new ArrayLoop<>(hosts);
    }

    static class SocketSender implements Runnable {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final BlockingQueue<DataChunk> queue;
        private final InetAddress firstInet4Address;
        private final String host;
        private final long lastChunkNumber;

        SocketSender(final BlockingQueue<DataChunk> queue, final InetAddress firstInet4Address, final String host, final long lastChunkNumber) {
            this.queue = queue;
            this.firstInet4Address = firstInet4Address;
            this.host = host;
            this.lastChunkNumber = lastChunkNumber;
        }

        @Override
        public void run() {
            try (Socket socket = new Socket(host, SenderTools.PORT, firstInet4Address, 0);
                 var dataOutputStream = new DataOutputStream(socket.getOutputStream())) {
                LOGGER.debug("Started a socket sender on local address {}", firstInet4Address);
                while (running.get()) {
                    final DataChunk dataChunk = queue.take();
                    if (dataChunk == END_OF_QUEUE) {
                        queue.put(END_OF_QUEUE);
                        LOGGER.info("Received end of queue marker, terminating socket sender for {}", firstInet4Address);
                        break;
                    }
                    LOGGER.debug("Sending data chunk #{}/{}", dataChunk.chunkNumber, lastChunkNumber);

                    dataOutputStream.writeLong(dataChunk.transactionId);
                    dataOutputStream.writeLong(dataChunk.chunkNumber);
                    dataOutputStream.write(dataChunk.block);
                    dataOutputStream.flush();
                }
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }

        }
    }
}
