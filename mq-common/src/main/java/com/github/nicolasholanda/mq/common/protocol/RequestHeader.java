package com.github.nicolasholanda.mq.common.protocol;

import io.netty.buffer.ByteBuf;

public record RequestHeader(ApiKey apiKey, short apiVersion, int correlationId, String clientId) {

    public void writeTo(ByteBuf buf) {
        buf.writeShort(apiKey.id());
        buf.writeShort(apiVersion);
        buf.writeInt(correlationId);
        Protocol.writeString(buf, clientId);
    }

    public static RequestHeader readFrom(ByteBuf buf) {
        ApiKey apiKey = ApiKey.forId(buf.readShort());
        short apiVersion = buf.readShort();
        int correlationId = buf.readInt();
        return new RequestHeader(apiKey, apiVersion, correlationId, Protocol.readString(buf));
    }
}
