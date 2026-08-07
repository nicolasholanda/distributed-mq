package com.github.nicolasholanda.mq.common.net;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.nicolasholanda.mq.common.protocol.ApiKey;
import com.github.nicolasholanda.mq.common.protocol.RequestHeader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import org.junit.jupiter.api.Test;

class FrameCodecTest {

    @Test
    void encodesLengthPrefixedFrames() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameEncoder());
        ByteBuf payload = Unpooled.buffer();
        payload.writeBytes(new byte[] {1, 2, 3, 4, 5});

        channel.writeOutbound(payload);
        ByteBuf encoded = channel.readOutbound();

        assertThat(encoded.readInt()).isEqualTo(5);
        assertThat(encoded.readableBytes()).isEqualTo(5);
        encoded.release();
    }

    @Test
    void decodesCompleteFrame() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());
        ByteBuf input = Unpooled.buffer();
        input.writeInt(4);
        input.writeBytes(new byte[] {9, 8, 7, 6});

        channel.writeInbound(input);
        ByteBuf frame = channel.readInbound();

        assertThat(frame.readableBytes()).isEqualTo(4);
        assertThat(frame.readByte()).isEqualTo((byte) 9);
        frame.release();
    }

    @Test
    void waitsForFragmentedFrame() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());
        ByteBuf firstHalf = Unpooled.buffer();
        firstHalf.writeInt(6);
        firstHalf.writeBytes(new byte[] {1, 2});

        channel.writeInbound(firstHalf);
        assertThat((Object) channel.readInbound()).isNull();

        channel.writeInbound(Unpooled.wrappedBuffer(new byte[] {3, 4, 5, 6}));
        ByteBuf frame = channel.readInbound();

        assertThat(frame.readableBytes()).isEqualTo(6);
        frame.release();
    }

    @Test
    void splitsBackToBackFrames() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());
        ByteBuf input = Unpooled.buffer();
        input.writeInt(2).writeBytes(new byte[] {1, 1});
        input.writeInt(3).writeBytes(new byte[] {2, 2, 2});

        channel.writeInbound(input);

        ByteBuf first = channel.readInbound();
        ByteBuf second = channel.readInbound();
        assertThat(first.readableBytes()).isEqualTo(2);
        assertThat(second.readableBytes()).isEqualTo(3);
        first.release();
        second.release();
    }

    @Test
    void rejectsOversizedFrame() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder(64));
        ByteBuf input = Unpooled.buffer();
        input.writeInt(1024);

        assertThatThrownBy(() -> channel.writeInbound(input)).isInstanceOf(CorruptedFrameException.class);
    }

    @Test
    void rejectsNegativeFrameSize() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameDecoder());
        ByteBuf input = Unpooled.buffer();
        input.writeInt(-7);

        assertThatThrownBy(() -> channel.writeInbound(input)).isInstanceOf(CorruptedFrameException.class);
    }

    @Test
    void roundTripsHeaderThroughBothCodecs() {
        EmbeddedChannel outbound = new EmbeddedChannel(new FrameEncoder());
        ByteBuf payload = Unpooled.buffer();
        new RequestHeader(ApiKey.FETCH, (short) 1, 42, "consumer-7").writeTo(payload);
        outbound.writeOutbound(payload);
        ByteBuf wire = outbound.readOutbound();

        EmbeddedChannel inbound = new EmbeddedChannel(new FrameDecoder());
        inbound.writeInbound(wire);
        ByteBuf frame = inbound.readInbound();
        RequestHeader header = RequestHeader.readFrom(frame);

        assertThat(header).isEqualTo(new RequestHeader(ApiKey.FETCH, (short) 1, 42, "consumer-7"));
        assertThat(frame.readableBytes()).isZero();
        frame.release();
    }
}
