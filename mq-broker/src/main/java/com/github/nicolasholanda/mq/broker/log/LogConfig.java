package com.github.nicolasholanda.mq.broker.log;

public record LogConfig(
        int segmentBytes,
        long segmentMs,
        int indexIntervalBytes,
        int maxIndexSize,
        int maxMessageBytes,
        long flushIntervalMessages) {

    public static final int DEFAULT_SEGMENT_BYTES = 1024 * 1024 * 1024;
    public static final long DEFAULT_SEGMENT_MS = 604_800_000L;
    public static final int DEFAULT_MAX_MESSAGE_BYTES = 1_048_588;

    public static LogConfig defaults() {
        return new LogConfig(DEFAULT_SEGMENT_BYTES, DEFAULT_SEGMENT_MS,
                LogSegment.DEFAULT_INDEX_INTERVAL_BYTES, LogSegment.DEFAULT_MAX_INDEX_SIZE,
                DEFAULT_MAX_MESSAGE_BYTES, Long.MAX_VALUE);
    }

    public LogConfig withSegmentBytes(int segmentBytes) {
        return new LogConfig(segmentBytes, segmentMs, indexIntervalBytes, maxIndexSize,
                maxMessageBytes, flushIntervalMessages);
    }

    public LogConfig withSegmentMs(long segmentMs) {
        return new LogConfig(segmentBytes, segmentMs, indexIntervalBytes, maxIndexSize,
                maxMessageBytes, flushIntervalMessages);
    }

    public LogConfig withIndexIntervalBytes(int indexIntervalBytes) {
        return new LogConfig(segmentBytes, segmentMs, indexIntervalBytes, maxIndexSize,
                maxMessageBytes, flushIntervalMessages);
    }

    public LogConfig withMaxIndexSize(int maxIndexSize) {
        return new LogConfig(segmentBytes, segmentMs, indexIntervalBytes, maxIndexSize,
                maxMessageBytes, flushIntervalMessages);
    }

    public LogConfig withMaxMessageBytes(int maxMessageBytes) {
        return new LogConfig(segmentBytes, segmentMs, indexIntervalBytes, maxIndexSize,
                maxMessageBytes, flushIntervalMessages);
    }
}
