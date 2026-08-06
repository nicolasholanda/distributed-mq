package com.github.nicolasholanda.mq.broker.log;

public class RecordTooLargeException extends RuntimeException {

    public RecordTooLargeException(String message) {
        super(message);
    }
}
