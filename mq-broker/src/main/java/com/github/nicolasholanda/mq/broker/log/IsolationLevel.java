package com.github.nicolasholanda.mq.broker.log;

public enum IsolationLevel {
    READ_UNCOMMITTED((byte) 0),
    READ_COMMITTED((byte) 1);

    private final byte id;

    IsolationLevel(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static IsolationLevel forId(byte id) {
        for (IsolationLevel level : values()) {
            if (level.id == id) {
                return level;
            }
        }
        throw new IllegalArgumentException("Unknown isolation level id: " + id);
    }
}
