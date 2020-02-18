package com.github.andremoniy.aws.multiple.eni.demo.daemon;

import com.github.andremoniy.aws.multiple.eni.demo.data.DataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class TransactionsManager implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionsManager.class);

    private final AtomicLong transactionCounter = new AtomicLong(1);
    private final Map<Long, Transaction> transactionMap = new ConcurrentHashMap<>();
    private final BlockingQueue<DataChunk> chunksQueue = new ArrayBlockingQueue<>(20);

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
                    executorService.submit(() -> transaction.processDataChunk(dataChunk));
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }
}
