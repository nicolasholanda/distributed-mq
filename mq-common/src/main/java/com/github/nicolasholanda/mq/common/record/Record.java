package com.github.nicolasholanda.mq.common.record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public record Record(
        byte attributes,
        long timestampDelta,
        int offsetDelta,
        byte[] key,
        byte[] value,
        List<RecordHeader> headers) {

    public static final int LENGTH_FIELD_SIZE = Integer.BYTES;

    public Record {
        headers = headers == null ? List.of() : List.copyOf(headers);
    }

    public static Record of(long timestampDelta, int offsetDelta, byte[] key, byte[] value) {
        return new Record((byte) 0, timestampDelta, offsetDelta, key, value, List.of());
    }

    public int bodySize() {
        int size = Byte.BYTES + Long.BYTES + Integer.BYTES;
        size += Integer.BYTES + (key == null ? 0 : key.length);
        size += Integer.BYTES + (value == null ? 0 : value.length);
        size += Integer.BYTES;
        for (RecordHeader header : headers) {
            size += header.sizeInBytes();
        }
        return size;
    }

    public int sizeInBytes() {
        return LENGTH_FIELD_SIZE + bodySize();
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(bodySize());
        buffer.put(attributes);
        buffer.putLong(timestampDelta);
        buffer.putInt(offsetDelta);
        writeBytes(buffer, key);
        writeBytes(buffer, value);
        buffer.putInt(headers.size());
        for (RecordHeader header : headers) {
            byte[] headerKey = header.key().getBytes(StandardCharsets.UTF_8);
            buffer.putInt(headerKey.length);
            buffer.put(headerKey);
            writeBytes(buffer, header.value());
        }
    }

    public static Record readFrom(ByteBuffer buffer) {
        int bodySize = buffer.getInt();
        if (buffer.remaining() < bodySize) {
            throw new CorruptRecordException("Record body truncated: expected "
                    + bodySize + " bytes but only " + buffer.remaining() + " available");
        }
        byte attributes = buffer.get();
        long timestampDelta = buffer.getLong();
        int offsetDelta = buffer.getInt();
        byte[] key = readBytes(buffer);
        byte[] value = readBytes(buffer);
        int headerCount = buffer.getInt();
        if (headerCount < 0) {
            throw new CorruptRecordException("Negative header count: " + headerCount);
        }
        List<RecordHeader> headers = new ArrayList<>(headerCount);
        for (int i = 0; i < headerCount; i++) {
            int keyLength = buffer.getInt();
            byte[] headerKey = new byte[keyLength];
            buffer.get(headerKey);
            headers.add(new RecordHeader(new String(headerKey, StandardCharsets.UTF_8), readBytes(buffer)));
        }
        return new Record(attributes, timestampDelta, offsetDelta, key, value, headers);
    }

    private static void writeBytes(ByteBuffer buffer, byte[] bytes) {
        if (bytes == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        }
    }

    private static byte[] readBytes(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > buffer.remaining()) {
            throw new CorruptRecordException("Invalid byte array length: " + length);
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }
}
