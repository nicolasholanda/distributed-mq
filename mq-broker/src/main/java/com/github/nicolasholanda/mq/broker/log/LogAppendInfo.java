package com.github.nicolasholanda.mq.broker.log;

public record LogAppendInfo(
        long baseOffset,
        long lastOffset,
        long maxTimestamp,
        long logAppendTime,
        int recordCount,
        int sizeInBytes) {
}
