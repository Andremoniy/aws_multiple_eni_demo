package com.github.andremoniy.aws.multiple.eni.demo.data;

public class DataChunk {

    public final long transactionId;
    public final long chunkNumber;
    public final byte[] block;

    public DataChunk(final long transactionId, final long chunkNumber, final byte[] block) {
        this.transactionId = transactionId;
        this.chunkNumber = chunkNumber;
        this.block = block;
    }
}
