package com.github.nicolasholanda.mq.common.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class FrameEncoder extends MessageToByteEncoder<ByteBuf> {

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf payload, ByteBuf out) {
        out.writeInt(payload.readableBytes());
        out.writeBytes(payload);
    }
}
