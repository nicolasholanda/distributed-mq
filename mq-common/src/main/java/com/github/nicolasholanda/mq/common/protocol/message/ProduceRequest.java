package com.github.nicolasholanda.mq.common.protocol.message;

import com.github.nicolasholanda.mq.common.protocol.Protocol;
import io.netty.buffer.ByteBuf;
import java.util.List;

public record ProduceRequest(
        short acks,
        int timeoutMs,
        String transactionalId,
        List<TopicData> topics) {

    public void writeTo(ByteBuf buf) {
        buf.writeShort(acks);
        buf.writeInt(timeoutMs);
        Protocol.writeString(buf, transactionalId);
        Protocol.writeArray(buf, topics, (target, topic) -> topic.writeTo(target));
    }

    public static ProduceRequest readFrom(ByteBuf buf) {
        short acks = buf.readShort();
        int timeoutMs = buf.readInt();
        String transactionalId = Protocol.readString(buf);
        return new ProduceRequest(acks, timeoutMs, transactionalId, Protocol.readArray(buf, TopicData::readFrom));
    }

    public record TopicData(String name, List<PartitionData> partitions) {

        public void writeTo(ByteBuf buf) {
            Protocol.writeString(buf, name);
            Protocol.writeArray(buf, partitions, (target, partition) -> partition.writeTo(target));
        }

        public static TopicData readFrom(ByteBuf buf) {
            return new TopicData(Protocol.readString(buf), Protocol.readArray(buf, PartitionData::readFrom));
        }
    }

    public record PartitionData(int index, byte[] records) {

        public void writeTo(ByteBuf buf) {
            buf.writeInt(index);
            Protocol.writeBytes(buf, records);
        }

        public static PartitionData readFrom(ByteBuf buf) {
            return new PartitionData(buf.readInt(), Protocol.readBytes(buf));
        }
    }
}
