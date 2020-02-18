package com.github.andremoniy.aws.multiple.eni.demo.daemon;

import com.github.andremoniy.aws.multiple.eni.demo.SenderTools;
import com.github.andremoniy.aws.multiple.eni.demo.data.DataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.andremoniy.aws.multiple.eni.demo.SenderTools.BLOCK_SIZE;

class SocketListener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketListener.class);
    
    private final TransactionsManager transactionsManager;
    private final InetAddress firstInet4Address;
    private final AtomicBoolean running = new AtomicBoolean(true);

    SocketListener(final TransactionsManager transactionsManager, final InetAddress firstInet4Address) {
        this.transactionsManager = transactionsManager;
        this.firstInet4Address = firstInet4Address;
    }

    @Override
    public void run() {
        try (var serverSocket = new ServerSocket(SenderTools.PORT, 0, firstInet4Address)) {
            LOGGER.info("Started server on port {} and inet address {}", SenderTools.PORT, firstInet4Address);
            do {
                final Socket accept = serverSocket.accept();
                LOGGER.info("Received a new connection on server on port {} and inet address {}", SenderTools.PORT, firstInet4Address);
                try (var dataOutputStream = new DataOutputStream(accept.getOutputStream());
                     var dataInputStream = new DataInputStream(accept.getInputStream())) {

                    // The reading loop
                    do {
                        final long transactionId = dataInputStream.readLong();

                        if (transactionId == 0) {
                            // A handshake
                            handshake(dataOutputStream, dataInputStream);
                            continue;
                        }

                        final int chunkNumber = dataInputStream.readInt();
                        final byte[] block = new byte[BLOCK_SIZE];
                        final int bytesRead = dataInputStream.read(block);
                        if (bytesRead != BLOCK_SIZE) {
                            LOGGER.warn("Broken chunk of data (#{}), expected: {}, received: {}", chunkNumber, BLOCK_SIZE, bytesRead);
                        }

                        try {
                            transactionsManager.process(new DataChunk(transactionId, chunkNumber, block));
                        } catch (InterruptedException e) {
                            LOGGER.error(e.getMessage(), e);
                            Thread.currentThread().interrupt();
                        }
                    } while (!serverSocket.isClosed());
                } catch (EOFException eof) {
                    LOGGER.info(eof.getMessage());
                }
            } while (running.get());
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public void handshake(DataOutputStream dataOutputStream, DataInputStream dataInputStream) throws IOException {
        final long size = dataInputStream.readLong();
        final String fileName = dataInputStream.readUTF();
        LOGGER.info("Starting a new transaction for a file of size {} and name {}", size, fileName);
        try {
            final long newTransaction = transactionsManager.registerNewTransaction(size, fileName);
            dataOutputStream.writeLong(newTransaction);
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage(), e);
            // To say that we failed to process this transaction
            dataOutputStream.writeLong(-1L);
        }
    }
}