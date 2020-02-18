package com.github.andremoniy.aws.multiple.eni.demo.daemon;

import com.github.andremoniy.aws.multiple.eni.demo.SenderTools;
import com.github.andremoniy.aws.multiple.eni.demo.data.DataChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.andremoniy.aws.multiple.eni.demo.SenderTools.BLOCK_SIZE;

class Transaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(Transaction.class);

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
        this.lastChunkNumber = SenderTools.getLastChunkNumber(size);
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

    private void writeChunkToDisk(final DataChunk dataChunk) throws IOException {
        final byte[] bytesToWrite;
        if (dataChunk.chunkNumber == lastChunkNumber) {
            bytesToWrite = new byte[SenderTools.getLastChunkSize(size, lastChunkNumber)];
            System.arraycopy(dataChunk.block, 0, bytesToWrite, 0, bytesToWrite.length);
        } else {
            bytesToWrite = dataChunk.block;
        }
        bufferedOutputStream.write(bytesToWrite);
    }

    long getId() {
        return id;
    }
}
