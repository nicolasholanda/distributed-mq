package com.github.nicolasholanda.mq.broker.log;

public record IndexEntry(long offset, int position) {

    public static final IndexEntry EMPTY = new IndexEntry(-1L, 0);
}
