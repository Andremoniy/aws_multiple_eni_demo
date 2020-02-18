package com.github.andremoniy.aws.multiple.eni.demo.daemon;

import com.github.andremoniy.aws.multiple.eni.demo.data.DataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class TransactionsManager implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionsManager.class);

    private final AtomicLong transactionCounter = new AtomicLong(1);
    private final Map<Long, Transaction> transactionMap = new ConcurrentHashMap<>();
    private final BlockingQueue<DataChunk> chunksQueue = new ArrayBlockingQueue<>(100);

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final boolean fakeWritings;

    public TransactionsManager(final boolean fakeWritings) {
        this.fakeWritings = fakeWritings;
    }

    long registerNewTransaction(final long size, final String fileName) {
        final Transaction newTransaction = new Transaction(
                transactionCounter.getAndIncrement(),
                size,
                fileName,
                fakeWritings
        );
        transactionMap.put(newTransaction.getId(), newTransaction);
        LOGGER.info("New file transaction started, id: {}, filename: {}, size: {}", newTransaction.getId(), fileName, size);
        return newTransaction.getId();
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
                    transaction.processDataChunk(dataChunk);
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
