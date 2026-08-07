package com.github.nicolasholanda.mq.common.protocol;

import java.util.Set;

public enum ErrorCode {
    UNKNOWN_SERVER_ERROR(-1),
    NONE(0),
    OFFSET_OUT_OF_RANGE(1),
    CORRUPT_MESSAGE(2),
    UNKNOWN_TOPIC_OR_PARTITION(3),
    LEADER_NOT_AVAILABLE(5),
    NOT_LEADER_OR_FOLLOWER(6),
    REQUEST_TIMED_OUT(7),
    REPLICA_NOT_AVAILABLE(9),
    MESSAGE_TOO_LARGE(10),
    NOT_ENOUGH_REPLICAS(19),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(20),
    ILLEGAL_GENERATION(22),
    UNKNOWN_MEMBER_ID(25),
    REBALANCE_IN_PROGRESS(27),
    TOPIC_ALREADY_EXISTS(36),
    DUPLICATE_SEQUENCE_NUMBER(45),
    OUT_OF_ORDER_SEQUENCE_NUMBER(46),
    FENCED_LEADER_EPOCH(74);

    private static final Set<ErrorCode> RETRIABLE = Set.of(
            LEADER_NOT_AVAILABLE,
            NOT_LEADER_OR_FOLLOWER,
            REQUEST_TIMED_OUT,
            REPLICA_NOT_AVAILABLE,
            NOT_ENOUGH_REPLICAS,
            REBALANCE_IN_PROGRESS,
            FENCED_LEADER_EPOCH);

    private final short code;

    ErrorCode(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    public boolean isRetriable() {
        return RETRIABLE.contains(this);
    }

    public static ErrorCode forCode(short code) {
        for (ErrorCode error : values()) {
            if (error.code == code) {
                return error;
            }
        }
        return UNKNOWN_SERVER_ERROR;
    }
}
