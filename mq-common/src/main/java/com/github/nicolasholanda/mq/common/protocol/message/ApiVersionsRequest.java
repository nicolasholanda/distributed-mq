package com.github.nicolasholanda.mq.common.protocol.message;

import com.github.nicolasholanda.mq.common.protocol.Protocol;
import io.netty.buffer.ByteBuf;

public record ApiVersionsRequest(String clientSoftwareName, String clientSoftwareVersion) {

    public void writeTo(ByteBuf buf) {
        Protocol.writeString(buf, clientSoftwareName);
        Protocol.writeString(buf, clientSoftwareVersion);
    }

    public static ApiVersionsRequest readFrom(ByteBuf buf) {
        return new ApiVersionsRequest(Protocol.readString(buf), Protocol.readString(buf));
    }
}
