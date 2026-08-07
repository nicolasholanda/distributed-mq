package com.github.nicolasholanda.mq.common.protocol;

public enum ApiKey {
    PRODUCE(0),
    FETCH(1),
    LIST_OFFSETS(2),
    METADATA(3),
    OFFSET_COMMIT(8),
    OFFSET_FETCH(9),
    FIND_COORDINATOR(10),
    JOIN_GROUP(11),
    HEARTBEAT(12),
    LEAVE_GROUP(13),
    SYNC_GROUP(14),
    API_VERSIONS(18),
    CREATE_TOPICS(19),
    DELETE_TOPICS(20),
    CONTROLLER_VOTE(60),
    CONTROLLER_APPEND(61);

    private final short id;

    ApiKey(int id) {
        this.id = (short) id;
    }

    public short id() {
        return id;
    }

    public static ApiKey forId(short id) {
        for (ApiKey key : values()) {
            if (key.id == id) {
                return key;
            }
        }
        throw new ProtocolException("Unknown api key: " + id);
    }
}
