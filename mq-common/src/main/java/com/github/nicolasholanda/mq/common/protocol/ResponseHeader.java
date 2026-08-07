package com.github.nicolasholanda.mq.common.protocol;

import io.netty.buffer.ByteBuf;

public record ResponseHeader(int correlationId) {

    public void writeTo(ByteBuf buf) {
        buf.writeInt(correlationId);
    }

    public static ResponseHeader readFrom(ByteBuf buf) {
        return new ResponseHeader(buf.readInt());
    }
}
