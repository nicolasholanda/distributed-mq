package com.github.nicolasholanda.mq.broker.log;

public class OffsetOutOfRangeException extends RuntimeException {

    public OffsetOutOfRangeException(String message) {
        super(message);
    }
}
