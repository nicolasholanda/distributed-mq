package com.github.nicolasholanda.mq.common.protocol.message;

import com.github.nicolasholanda.mq.common.protocol.ApiKey;
import com.github.nicolasholanda.mq.common.protocol.ErrorCode;
import com.github.nicolasholanda.mq.common.protocol.Protocol;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;

public record ApiVersionsResponse(ErrorCode errorCode, List<ApiVersion> apiVersions, int throttleTimeMs) {

    public static ApiVersionsResponse supporting(ApiKey... keys) {
        List<ApiVersion> versions = new ArrayList<>();
        for (ApiKey key : keys) {
            versions.add(new ApiVersion(key, (short) 0, (short) 0));
        }
        return new ApiVersionsResponse(ErrorCode.NONE, versions, 0);
    }

    public void writeTo(ByteBuf buf) {
        buf.writeShort(errorCode.code());
        Protocol.writeArray(buf, apiVersions, (target, version) -> version.writeTo(target));
        buf.writeInt(throttleTimeMs);
    }

    public static ApiVersionsResponse readFrom(ByteBuf buf) {
        ErrorCode errorCode = ErrorCode.forCode(buf.readShort());
        List<ApiVersion> versions = Protocol.readArray(buf, ApiVersion::readFrom);
        return new ApiVersionsResponse(errorCode, versions, buf.readInt());
    }

    public record ApiVersion(ApiKey apiKey, short minVersion, short maxVersion) {

        public void writeTo(ByteBuf buf) {
            buf.writeShort(apiKey.id());
            buf.writeShort(minVersion);
            buf.writeShort(maxVersion);
        }

        public static ApiVersion readFrom(ByteBuf buf) {
            return new ApiVersion(ApiKey.forId(buf.readShort()), buf.readShort(), buf.readShort());
        }
    }
}
