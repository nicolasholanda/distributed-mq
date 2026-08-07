package com.github.nicolasholanda.mq.common.protocol.message;

import com.github.nicolasholanda.mq.common.protocol.Protocol;
import io.netty.buffer.ByteBuf;
import java.util.List;

public record FetchRequest(
        int replicaId,
        int maxWaitMs,
        int minBytes,
        int maxBytes,
        byte isolationLevel,
        List<TopicData> topics) {

    public static final int CONSUMER_REPLICA_ID = -1;

    public boolean isFromFollower() {
        return replicaId >= 0;
    }

    public void writeTo(ByteBuf buf) {
        buf.writeInt(replicaId);
        buf.writeInt(maxWaitMs);
        buf.writeInt(minBytes);
        buf.writeInt(maxBytes);
        buf.writeByte(isolationLevel);
        Protocol.writeArray(buf, topics, (target, topic) -> topic.writeTo(target));
    }

    public static FetchRequest readFrom(ByteBuf buf) {
        int replicaId = buf.readInt();
        int maxWaitMs = buf.readInt();
        int minBytes = buf.readInt();
        int maxBytes = buf.readInt();
        byte isolationLevel = buf.readByte();
        return new FetchRequest(replicaId, maxWaitMs, minBytes, maxBytes, isolationLevel,
                Protocol.readArray(buf, TopicData::readFrom));
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

    public record PartitionData(
            int index,
            int currentLeaderEpoch,
            long fetchOffset,
            long logStartOffset,
            int partitionMaxBytes) {

        public void writeTo(ByteBuf buf) {
            buf.writeInt(index);
            buf.writeInt(currentLeaderEpoch);
            buf.writeLong(fetchOffset);
            buf.writeLong(logStartOffset);
            buf.writeInt(partitionMaxBytes);
        }

        public static PartitionData readFrom(ByteBuf buf) {
            return new PartitionData(buf.readInt(), buf.readInt(), buf.readLong(), buf.readLong(), buf.readInt());
        }
    }
}
