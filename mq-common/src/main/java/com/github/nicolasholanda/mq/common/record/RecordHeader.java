package com.github.nicolasholanda.mq.common.record;

import java.nio.charset.StandardCharsets;

public record RecordHeader(String key, byte[] value) {

    public int sizeInBytes() {
        int keyLength = key.getBytes(StandardCharsets.UTF_8).length;
        return Integer.BYTES + keyLength + Integer.BYTES + (value == null ? 0 : value.length);
    }
}
