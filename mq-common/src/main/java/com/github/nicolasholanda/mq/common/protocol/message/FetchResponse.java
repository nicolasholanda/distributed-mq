package com.github.nicolasholanda.mq.common.protocol.message;

import com.github.nicolasholanda.mq.common.protocol.ErrorCode;
import com.github.nicolasholanda.mq.common.protocol.Protocol;
import io.netty.buffer.ByteBuf;
import java.util.List;

public record FetchResponse(int throttleTimeMs, List<TopicResponse> topics) {

    public void writeTo(ByteBuf buf) {
        buf.writeInt(throttleTimeMs);
        Protocol.writeArray(buf, topics, (target, topic) -> topic.writeTo(target));
    }

    public static FetchResponse readFrom(ByteBuf buf) {
        int throttleTimeMs = buf.readInt();
        return new FetchResponse(throttleTimeMs, Protocol.readArray(buf, TopicResponse::readFrom));
    }

    public record TopicResponse(String name, List<PartitionResponse> partitions) {

        public void writeTo(ByteBuf buf) {
            Protocol.writeString(buf, name);
            Protocol.writeArray(buf, partitions, (target, partition) -> partition.writeTo(target));
        }

        public static TopicResponse readFrom(ByteBuf buf) {
            return new TopicResponse(Protocol.readString(buf), Protocol.readArray(buf, PartitionResponse::readFrom));
        }
    }

    public record PartitionResponse(
            int index,
            ErrorCode errorCode,
            long highWatermark,
            long logStartOffset,
            byte[] records) {

        public void writeTo(ByteBuf buf) {
            buf.writeInt(index);
            buf.writeShort(errorCode.code());
            buf.writeLong(highWatermark);
            buf.writeLong(logStartOffset);
            Protocol.writeBytes(buf, records);
        }

        public static PartitionResponse readFrom(ByteBuf buf) {
            return new PartitionResponse(buf.readInt(), ErrorCode.forCode(buf.readShort()),
                    buf.readLong(), buf.readLong(), Protocol.readBytes(buf));
        }
    }
}
