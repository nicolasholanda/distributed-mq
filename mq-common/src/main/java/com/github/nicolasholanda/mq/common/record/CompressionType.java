package com.github.nicolasholanda.mq.common.record;

public enum CompressionType {
    NONE(0),
    GZIP(1),
    LZ4(2);

    private final int id;

    CompressionType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static CompressionType forId(int id) {
        for (CompressionType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown compression type id: " + id);
    }
}
