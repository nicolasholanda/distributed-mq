package com.github.nicolasholanda.mq.common.protocol.message;

import com.github.nicolasholanda.mq.common.protocol.ErrorCode;
import com.github.nicolasholanda.mq.common.protocol.Protocol;
import io.netty.buffer.ByteBuf;
import java.util.List;

public record ProduceResponse(List<TopicResponse> topics, int throttleTimeMs) {

    public void writeTo(ByteBuf buf) {
        Protocol.writeArray(buf, topics, (target, topic) -> topic.writeTo(target));
        buf.writeInt(throttleTimeMs);
    }

    public static ProduceResponse readFrom(ByteBuf buf) {
        List<TopicResponse> topics = Protocol.readArray(buf, TopicResponse::readFrom);
        return new ProduceResponse(topics, buf.readInt());
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
            long baseOffset,
            long logAppendTimeMs,
            long logStartOffset) {

        public void writeTo(ByteBuf buf) {
            buf.writeInt(index);
            buf.writeShort(errorCode.code());
            buf.writeLong(baseOffset);
            buf.writeLong(logAppendTimeMs);
            buf.writeLong(logStartOffset);
        }

        public static PartitionResponse readFrom(ByteBuf buf) {
            return new PartitionResponse(buf.readInt(), ErrorCode.forCode(buf.readShort()),
                    buf.readLong(), buf.readLong(), buf.readLong());
        }
    }
}
