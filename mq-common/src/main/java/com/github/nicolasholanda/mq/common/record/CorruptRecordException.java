package com.github.nicolasholanda.mq.common.record;

public class CorruptRecordException extends RuntimeException {

    public CorruptRecordException(String message) {
        super(message);
    }
}
