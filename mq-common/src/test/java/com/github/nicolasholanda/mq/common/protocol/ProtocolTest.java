package com.github.nicolasholanda.mq.common.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProtocolTest {

    @Test
    void roundTripsStrings() {
        ByteBuf buf = Unpooled.buffer();
        Protocol.writeString(buf, "orders");
        Protocol.writeString(buf, null);
        Protocol.writeString(buf, "");

        assertThat(Protocol.readString(buf)).isEqualTo("orders");
        assertThat(Protocol.readString(buf)).isNull();
        assertThat(Protocol.readString(buf)).isEmpty();
    }

    @Test
    void roundTripsBytes() {
        ByteBuf buf = Unpooled.buffer();
        Protocol.writeBytes(buf, new byte[] {1, 2, 3});
        Protocol.writeBytes(buf, (byte[]) null);

        assertThat(Protocol.readBytes(buf)).containsExactly(1, 2, 3);
        assertThat(Protocol.readBytes(buf)).isNull();
    }

    @Test
    void roundTripsArrays() {
        ByteBuf buf = Unpooled.buffer();
        Protocol.writeArray(buf, List.of("a", "b", "c"), Protocol::writeString);
        Protocol.writeArray(buf, null, Protocol::writeString);
        Protocol.writeArray(buf, List.of(), Protocol::writeString);

        assertThat(Protocol.readArray(buf, Protocol::readString)).containsExactly("a", "b", "c");
        assertThat(Protocol.readArray(buf, Protocol::readString)).isNull();
        assertThat(Protocol.readArray(buf, Protocol::readString)).isEmpty();
    }

    @Test
    void roundTripsBooleans() {
        ByteBuf buf = Unpooled.buffer();
        Protocol.writeBoolean(buf, true);
        Protocol.writeBoolean(buf, false);

        assertThat(Protocol.readBoolean(buf)).isTrue();
        assertThat(Protocol.readBoolean(buf)).isFalse();
    }

    @Test
    void rejectsTruncatedPayload() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(50);
        buf.writeBytes(new byte[10]);

        assertThatThrownBy(() -> Protocol.readString(buf))
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("underflow");
    }

    @Test
    void roundTripsRequestHeader() {
        ByteBuf buf = Unpooled.buffer();
        new RequestHeader(ApiKey.PRODUCE, (short) 3, 77, "producer-1").writeTo(buf);

        RequestHeader header = RequestHeader.readFrom(buf);

        assertThat(header.apiKey()).isEqualTo(ApiKey.PRODUCE);
        assertThat(header.apiVersion()).isEqualTo((short) 3);
        assertThat(header.correlationId()).isEqualTo(77);
        assertThat(header.clientId()).isEqualTo("producer-1");
    }

    @Test
    void roundTripsResponseHeader() {
        ByteBuf buf = Unpooled.buffer();
        new ResponseHeader(9001).writeTo(buf);

        assertThat(ResponseHeader.readFrom(buf).correlationId()).isEqualTo(9001);
    }

    @Test
    void apiKeysUseKafkaNumbering() {
        assertThat(ApiKey.PRODUCE.id()).isZero();
        assertThat(ApiKey.FETCH.id()).isEqualTo((short) 1);
        assertThat(ApiKey.API_VERSIONS.id()).isEqualTo((short) 18);
        assertThat(ApiKey.CONTROLLER_APPEND.id()).isEqualTo((short) 61);
        assertThat(ApiKey.forId((short) 19)).isEqualTo(ApiKey.CREATE_TOPICS);
        assertThatThrownBy(() -> ApiKey.forId((short) 99)).isInstanceOf(ProtocolException.class);
    }

    @Test
    void errorCodesMatchSpecAndFlagRetriable() {
        assertThat(ErrorCode.NONE.code()).isZero();
        assertThat(ErrorCode.UNKNOWN_SERVER_ERROR.code()).isEqualTo((short) -1);
        assertThat(ErrorCode.forCode((short) 6)).isEqualTo(ErrorCode.NOT_LEADER_OR_FOLLOWER);
        assertThat(ErrorCode.forCode((short) 12345)).isEqualTo(ErrorCode.UNKNOWN_SERVER_ERROR);

        assertThat(ErrorCode.NOT_LEADER_OR_FOLLOWER.isRetriable()).isTrue();
        assertThat(ErrorCode.REBALANCE_IN_PROGRESS.isRetriable()).isTrue();
        assertThat(ErrorCode.FENCED_LEADER_EPOCH.isRetriable()).isTrue();
        assertThat(ErrorCode.CORRUPT_MESSAGE.isRetriable()).isFalse();
        assertThat(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION.isRetriable()).isFalse();
    }
}
