package com.github.andremoniy.aws.multiple.eni.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 *
 */
public class SenderDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(SenderDaemon.class);
    static final int BLOCK_SIZE = 8500;
    static final int PORT = 8080;

    public static void main(String[] args) throws IOException {
        // 1. list of ENIs

        final List<NetworkInterface> networkInterfaces = NetworkInterface
                .networkInterfaces()
                .filter(networkInterface -> !networkInterface.isVirtual())
                .filter(networkInterface -> networkInterface.getName().startsWith("e"))
                .collect(Collectors.toUnmodifiableList());

        LOGGER.info("Found {} network interfaces", networkInterfaces.size());

        final var executorService = Executors.newFixedThreadPool(networkInterfaces.size() * 2 + 1);
        final TransactionsManager transactionsManager = new TransactionsManager(executorService);
        executorService.submit(transactionsManager);

        final List<Future<?>> futures = new ArrayList<>();

        for (var networkInterface : networkInterfaces) {
            final Optional<InetAddress> firstInet4Address = networkInterface
                    .inetAddresses()
                    .filter(inetAddress -> inetAddress instanceof Inet4Address)
                    .findFirst();

            if (firstInet4Address.isEmpty()) {
                LOGGER.info("There is no Inet4 address in the interface {}, skipping", networkInterface);
                continue;
            }

            final Future<?> future = executorService.submit(() -> {
                try {
                    final var serverSocket = new ServerSocket(PORT, 0, firstInet4Address.get());
                    LOGGER.info("Started server on port {} and inet address {}", PORT, firstInet4Address.get());
                    do {
                        final Socket accept = serverSocket.accept();
                        LOGGER.info("Received a new connection on server on port {} and inet address {}", PORT, firstInet4Address.get());
                        try (var dataOutputStream = new DataOutputStream(accept.getOutputStream());
                             var dataInputStream = new DataInputStream(accept.getInputStream())) {

                            do {
                                final long transactionId = dataInputStream.readLong();
                                if (transactionId == 0) {
                                    final long size = dataInputStream.readLong();
                                    final String fileName = dataInputStream.readUTF();
                                    try {
                                        final long newTransaction = transactionsManager.registerNewTransaction(size, fileName);
                                        dataOutputStream.writeLong(newTransaction);
                                    } catch (RuntimeException e) {
                                        LOGGER.error(e.getMessage(), e);
                                        // To say that we failed to process this transaction
                                        dataOutputStream.writeLong(-1L);
                                    }
                                } else {

                                    final int chunkNumber = dataInputStream.readInt();
                                    final byte[] block = new byte[BLOCK_SIZE];
                                    dataInputStream.read(block);

                                    try {
                                        transactionsManager.process(new DataChunk(transactionId, chunkNumber, block));
                                    } catch (InterruptedException e) {
                                        LOGGER.error(e.getMessage(), e);
                                        Thread.currentThread().interrupt();
                                    }
                                }
                            } while (!serverSocket.isClosed());
                        } catch (EOFException eof) {
                            LOGGER.info(eof.getMessage());
                        }
                    } while (true);
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            });

            futures.add(future);
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOGGER.error(e.getMessage(), e);
            }
        });
    }

    static int getLastChunkSize(long size, int lastChunkNumber) {
        return (int) (size - BLOCK_SIZE * (lastChunkNumber - 1));
    }

    static class DataChunk {
        final long transactionId;
        final int chunkNumber;
        final byte[] block;

        DataChunk(final long transactionId, final int chunkNumber, final byte[] block) {
            this.transactionId = transactionId;
            this.chunkNumber = chunkNumber;
            this.block = block;
        }
    }

    static class Transaction {
        private final long id;
        private final long size;
        private final String fileName;
        private final int lastChunkNumber;
        private final BufferedOutputStream bufferedOutputStream;
        private final AtomicInteger lastWrittenChunkNumber = new AtomicInteger(0);

        private final List<DataChunk> unprocessedChunks = new ArrayList<>();

        Transaction(final long id, final long size, final String fileName) {
            this.id = id;
            this.size = size;
            this.fileName = fileName;
            this.lastChunkNumber = getLastChunkNumber(size);
            try {
                this.bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("/tmp/" + fileName), BLOCK_SIZE);
            } catch (FileNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }

        synchronized boolean processDataChunk(final DataChunk dataChunk) throws IOException {
            LOGGER.info("Processing chunk #{} of file {}", dataChunk.chunkNumber, fileName);
            if (dataChunk.chunkNumber == lastWrittenChunkNumber.get() + 1) {
                DataChunk dataChunkToProcess = dataChunk;

                while (dataChunkToProcess != null) {
                    writeChunkToDisk(dataChunk);
                    lastWrittenChunkNumber.set(dataChunk.chunkNumber);
                    if (dataChunk.chunkNumber == lastChunkNumber) {
                        bufferedOutputStream.flush();
                        bufferedOutputStream.close();
                        LOGGER.info("Finished transaction {} for file {}", id, fileName);
                        return true;
                    } else {
                        dataChunkToProcess = findNextDataChunk();
                    }
                }
            } else {
                unprocessedChunks.add(dataChunk);
            }
            return false;
        }

        private DataChunk findNextDataChunk() {
            DataChunk dataChunkToProcess;
            dataChunkToProcess = null;
            for (DataChunk unprocessedDataChunk : unprocessedChunks) {
                if (unprocessedDataChunk.chunkNumber == lastWrittenChunkNumber.get() + 1) {
                    dataChunkToProcess = unprocessedDataChunk;
                    break;
                }
            }
            return dataChunkToProcess;
        }

        void writeChunkToDisk(final DataChunk dataChunk) throws IOException {
            final byte[] bytesToWrite;
            if (dataChunk.chunkNumber == lastChunkNumber) {
                bytesToWrite = new byte[getLastChunkSize(size, lastChunkNumber)];
                System.arraycopy(dataChunk.block, 0, bytesToWrite, 0, bytesToWrite.length);
            } else {
                bytesToWrite = dataChunk.block;
            }
            bufferedOutputStream.write(bytesToWrite);
        }

    }

    static int getLastChunkNumber(double size) {
        return (int) Math.ceil(size / BLOCK_SIZE);
    }

    static class TransactionsManager implements Runnable {

        private final AtomicLong transactionCounter = new AtomicLong(1);
        private final Map<Long, Transaction> transactionMap = new ConcurrentHashMap<>();
        private final BlockingQueue<DataChunk> chunksQueue = new ArrayBlockingQueue<>(100);

        private final ExecutorService executorService;
        private final AtomicBoolean running = new AtomicBoolean(true);

        TransactionsManager(final ExecutorService executorService) {
            this.executorService = executorService;
        }

        long registerNewTransaction(final long size, final String fileName) {
            final Transaction newTransaction = new Transaction(
                    transactionCounter.getAndIncrement(),
                    size,
                    fileName
            );
            transactionMap.put(newTransaction.id, newTransaction);
            LOGGER.info("New file transaction started, id: {}, filename: {}, size: {}", newTransaction.id, fileName, size);
            return newTransaction.id;
        }

        void process(final DataChunk dataChunk) throws InterruptedException {
            chunksQueue.put(dataChunk);
        }

        @Override
        public void run() {
            try {
                while (running.get()) {
                    final DataChunk dataChunk = chunksQueue.take();
                    final Transaction transaction = transactionMap.get(dataChunk.transactionId);
                    if (transaction == null) {
                        LOGGER.error("Cannot find transaction with id {}", dataChunk.transactionId);
                    } else {
                        executorService.submit(() -> transaction.processDataChunk(dataChunk));
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
        }
    }

}
