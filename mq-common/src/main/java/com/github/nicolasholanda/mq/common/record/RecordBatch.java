package com.github.nicolasholanda.mq.common.record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public record RecordBatch(
        long baseOffset,
        int partitionLeaderEpoch,
        byte magic,
        short attributes,
        long baseTimestamp,
        long maxTimestamp,
        long producerId,
        short producerEpoch,
        int baseSequence,
        List<Record> records) {

    public static final byte CURRENT_MAGIC = 2;
    public static final int HEADER_SIZE = 61;

    public static final int BASE_OFFSET_OFFSET = 0;
    public static final int BATCH_LENGTH_OFFSET = 8;
    public static final int PARTITION_LEADER_EPOCH_OFFSET = 12;
    public static final int MAGIC_OFFSET = 16;
    public static final int CRC_OFFSET = 17;
    public static final int ATTRIBUTES_OFFSET = 21;

    public static final short CONTROL_FLAG = 0x0008;
    public static final long NO_PRODUCER_ID = -1L;
    public static final short NO_PRODUCER_EPOCH = -1;
    public static final int NO_SEQUENCE = -1;

    public RecordBatch {
        records = List.copyOf(records);
    }

    public int recordCount() {
        return records.size();
    }

    public int lastOffsetDelta() {
        return records.size() - 1;
    }

    public long lastOffset() {
        return baseOffset + lastOffsetDelta();
    }

    public CompressionType compressionType() {
        return CompressionType.forId(attributes & 0x0007);
    }

    public boolean isControlBatch() {
        return (attributes & CONTROL_FLAG) != 0;
    }

    public boolean isIdempotent() {
        return producerId != NO_PRODUCER_ID;
    }

    public int sizeInBytes() {
        int size = HEADER_SIZE;
        for (Record record : records) {
            size += record.sizeInBytes();
        }
        return size;
    }

    public RecordBatch withBaseOffset(long newBaseOffset) {
        return new RecordBatch(newBaseOffset, partitionLeaderEpoch, magic, attributes,
                baseTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, records);
    }

    public RecordBatch withPartitionLeaderEpoch(int epoch) {
        return new RecordBatch(baseOffset, epoch, magic, attributes,
                baseTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, records);
    }

    public ByteBuffer toBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes());
        writeTo(buffer);
        buffer.flip();
        return buffer;
    }

    public void writeTo(ByteBuffer buffer) {
        int start = buffer.position();
        buffer.putLong(baseOffset);
        buffer.putInt(sizeInBytes() - BATCH_LENGTH_OFFSET - Integer.BYTES);
        buffer.putInt(partitionLeaderEpoch);
        buffer.put(magic);
        buffer.putInt(0);
        buffer.putShort(attributes);
        buffer.putInt(lastOffsetDelta());
        buffer.putLong(baseTimestamp);
        buffer.putLong(maxTimestamp);
        buffer.putLong(producerId);
        buffer.putShort(producerEpoch);
        buffer.putInt(baseSequence);
        buffer.putInt(records.size());
        for (Record record : records) {
            record.writeTo(buffer);
        }
        int end = buffer.position();
        int crcStart = start + ATTRIBUTES_OFFSET;
        long crc = Crc32C.compute(buffer, crcStart, end - crcStart);
        buffer.putInt(start + CRC_OFFSET, (int) crc);
    }

    public static RecordBatch readFrom(ByteBuffer buffer) {
        int start = buffer.position();
        if (buffer.remaining() < HEADER_SIZE) {
            throw new CorruptRecordException("Batch header truncated: only " + buffer.remaining() + " bytes left");
        }
        long baseOffset = buffer.getLong();
        int batchLength = buffer.getInt();
        int totalSize = batchLength + BATCH_LENGTH_OFFSET + Integer.BYTES;
        if (batchLength < HEADER_SIZE - BATCH_LENGTH_OFFSET - Integer.BYTES
                || buffer.limit() - start < totalSize) {
            throw new CorruptRecordException("Batch truncated: declared length " + batchLength
                    + " but only " + (buffer.limit() - start) + " bytes available");
        }
        int partitionLeaderEpoch = buffer.getInt();
        byte magic = buffer.get();
        if (magic != CURRENT_MAGIC) {
            throw new CorruptRecordException("Unsupported magic byte: " + magic);
        }
        long storedCrc = buffer.getInt() & 0xFFFFFFFFL;
        int crcStart = start + ATTRIBUTES_OFFSET;
        long computedCrc = Crc32C.compute(buffer, crcStart, totalSize - ATTRIBUTES_OFFSET);
        if (storedCrc != computedCrc) {
            throw new CorruptRecordException("CRC mismatch: stored " + storedCrc + " computed " + computedCrc);
        }
        short attributes = buffer.getShort();
        buffer.getInt();
        long baseTimestamp = buffer.getLong();
        long maxTimestamp = buffer.getLong();
        long producerId = buffer.getLong();
        short producerEpoch = buffer.getShort();
        int baseSequence = buffer.getInt();
        int recordCount = buffer.getInt();
        if (recordCount < 0) {
            throw new CorruptRecordException("Negative record count: " + recordCount);
        }
        List<Record> records = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            records.add(Record.readFrom(buffer));
        }
        buffer.position(start + totalSize);
        return new RecordBatch(baseOffset, partitionLeaderEpoch, magic, attributes,
                baseTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, records);
    }

    public static int peekBatchLength(ByteBuffer buffer, int position) {
        return buffer.getInt(position + BATCH_LENGTH_OFFSET) + BATCH_LENGTH_OFFSET + Integer.BYTES;
    }
}
