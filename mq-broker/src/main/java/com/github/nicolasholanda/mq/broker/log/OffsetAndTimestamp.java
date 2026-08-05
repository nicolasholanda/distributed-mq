package com.github.nicolasholanda.mq.broker.log;

public record OffsetAndTimestamp(long offset, long timestamp) {

    public static final OffsetAndTimestamp NONE = new OffsetAndTimestamp(-1L, -1L);
}
