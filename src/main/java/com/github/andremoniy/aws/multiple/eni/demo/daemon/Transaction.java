package com.github.andremoniy.aws.multiple.eni.demo.daemon;

import com.github.andremoniy.aws.multiple.eni.demo.data.DataChunk;
import com.github.andremoniy.aws.multiple.eni.demo.util.SenderTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.andremoniy.aws.multiple.eni.demo.util.SenderTools.BLOCK_SIZE;

class Transaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(Transaction.class);

    private final long id;
    private final long size;
    private final String fileName;
    private final long lastChunkNumber;
    private final BufferedOutputStream bufferedOutputStream;
    private final AtomicLong lastWrittenChunkNumber = new AtomicLong(0);

    private final Set<DataChunk> unprocessedChunks = new HashSet<>();
    private final boolean fakeWritings;

    Transaction(final long id, final long size, final String fileName, boolean fakeWritings) {
        this.id = id;
        this.size = size;
        this.fileName = fileName;
        this.lastChunkNumber = SenderTools.getLastChunkNumber(size);
        this.fakeWritings = fakeWritings;
        try {
            this.bufferedOutputStream = new BufferedOutputStream(new FileOutputStream("/tmp/" + fileName), BLOCK_SIZE);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    void processDataChunk(final DataChunk dataChunk) throws IOException {
        LOGGER.debug("Processing chunk #{} of file {}", dataChunk.chunkNumber, fileName);
        final long expectedChunkNumber = getExpectedChunkNumber();
        if (dataChunk.chunkNumber == expectedChunkNumber) {
            DataChunk dataChunkToProcess = dataChunk;

            while (dataChunkToProcess != null) {
                LOGGER.debug("Writing chunk #{} to disk", dataChunkToProcess.chunkNumber);
                writeChunkToDisk(dataChunkToProcess);
                lastWrittenChunkNumber.set(dataChunkToProcess.chunkNumber);
                if (dataChunkToProcess.chunkNumber == lastChunkNumber) {
                    bufferedOutputStream.flush();
                    bufferedOutputStream.close();
                    LOGGER.info("Finished transaction {} for file {}", id, fileName);
                    return;
                } else {
                    dataChunkToProcess = findNextDataChunk();
                }
            }
        } else {
            LOGGER.debug("Storing chunk #{} for late processing, expected next chunk: #{}, current queue size: {}", dataChunk.chunkNumber, expectedChunkNumber, unprocessedChunks.size());
            unprocessedChunks.add(dataChunk);
        }
    }

    private long getExpectedChunkNumber() {
        return lastWrittenChunkNumber.get() + 1;
    }

    private DataChunk findNextDataChunk() {
        for (Iterator<DataChunk> iterator = unprocessedChunks.iterator(); iterator.hasNext(); ) {
            DataChunk unprocessedDataChunk = iterator.next();
            if (unprocessedDataChunk.chunkNumber == getExpectedChunkNumber()) {
                iterator.remove();
                return unprocessedDataChunk;
            }
        }
        return null;
    }

    private void writeChunkToDisk(final DataChunk dataChunk) throws IOException {
        final byte[] bytesToWrite;
        if (dataChunk.chunkNumber == lastChunkNumber) {
            bytesToWrite = new byte[SenderTools.getLastChunkSize(size, lastChunkNumber)];
            System.arraycopy(dataChunk.block, 0, bytesToWrite, 0, bytesToWrite.length);
        } else {
            bytesToWrite = dataChunk.block;
        }
        if (!fakeWritings) {
            bufferedOutputStream.write(bytesToWrite);
        }
    }

    long getId() {
        return id;
    }
}
