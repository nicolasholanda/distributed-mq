package com.github.nicolasholanda.mq.common.record;

import java.util.ArrayList;
import java.util.List;

public final class RecordBatchBuilder {

    private long baseOffset;
    private int partitionLeaderEpoch = -1;
    private CompressionType compressionType = CompressionType.NONE;
    private boolean control;
    private long baseTimestamp = -1;
    private long maxTimestamp = -1;
    private long producerId = RecordBatch.NO_PRODUCER_ID;
    private short producerEpoch = RecordBatch.NO_PRODUCER_EPOCH;
    private int baseSequence = RecordBatch.NO_SEQUENCE;
    private final List<Record> records = new ArrayList<>();

    public RecordBatchBuilder baseOffset(long baseOffset) {
        this.baseOffset = baseOffset;
        return this;
    }

    public RecordBatchBuilder partitionLeaderEpoch(int partitionLeaderEpoch) {
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        return this;
    }

    public RecordBatchBuilder compressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    public RecordBatchBuilder control(boolean control) {
        this.control = control;
        return this;
    }

    public RecordBatchBuilder producer(long producerId, short producerEpoch, int baseSequence) {
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        return this;
    }

    public RecordBatchBuilder append(long timestamp, byte[] key, byte[] value) {
        return append(timestamp, key, value, List.of());
    }

    public RecordBatchBuilder append(long timestamp, byte[] key, byte[] value, List<RecordHeader> headers) {
        if (baseTimestamp < 0) {
            baseTimestamp = timestamp;
        }
        maxTimestamp = Math.max(maxTimestamp, timestamp);
        records.add(new Record((byte) 0, timestamp - baseTimestamp, records.size(), key, value, headers));
        return this;
    }

    public int recordCount() {
        return records.size();
    }

    public RecordBatch build() {
        if (records.isEmpty()) {
            throw new IllegalStateException("Cannot build an empty record batch");
        }
        short attributes = (short) compressionType.id();
        if (control) {
            attributes |= RecordBatch.CONTROL_FLAG;
        }
        return new RecordBatch(baseOffset, partitionLeaderEpoch, RecordBatch.CURRENT_MAGIC, attributes,
                baseTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, records);
    }
}
