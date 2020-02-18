package com.github.andremoniy.aws.multiple.eni.demo.data;

public class DataChunk {

    public final long transactionId;
    public final int chunkNumber;
    public final byte[] block;

    public DataChunk(final long transactionId, final int chunkNumber, final byte[] block) {
        this.transactionId = transactionId;
        this.chunkNumber = chunkNumber;
        this.block = block;
    }
}
