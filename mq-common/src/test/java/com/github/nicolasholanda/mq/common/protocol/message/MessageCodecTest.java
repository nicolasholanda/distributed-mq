package com.github.nicolasholanda.mq.common.protocol.message;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.nicolasholanda.mq.common.protocol.ApiKey;
import com.github.nicolasholanda.mq.common.protocol.ErrorCode;
import com.github.nicolasholanda.mq.common.record.RecordBatch;
import com.github.nicolasholanda.mq.common.record.RecordBatchBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class MessageCodecTest {

    @Test
    void roundTripsProduceRequestCarryingRawBatches() {
        byte[] rawBatch = rawBatch("payload");
        ProduceRequest request = new ProduceRequest((short) -1, 30_000, null, List.of(
                new ProduceRequest.TopicData("orders", List.of(
                        new ProduceRequest.PartitionData(0, rawBatch),
                        new ProduceRequest.PartitionData(1, rawBatch)))));

        ProduceRequest decoded = roundTrip(request::writeTo, ProduceRequest::readFrom);

        assertThat(decoded.acks()).isEqualTo((short) -1);
        assertThat(decoded.timeoutMs()).isEqualTo(30_000);
        assertThat(decoded.transactionalId()).isNull();
        assertThat(decoded.topics()).singleElement()
                .satisfies(topic -> assertThat(topic.name()).isEqualTo("orders"));
        assertThat(decoded.topics().getFirst().partitions()).hasSize(2);
        assertThat(decoded.topics().getFirst().partitions().getFirst().records()).isEqualTo(rawBatch);
    }

    @Test
    void produceRequestPayloadStaysDecodableAsRecordBatch() {
        byte[] rawBatch = rawBatch("still-valid");
        ProduceRequest request = new ProduceRequest((short) 1, 1_000, null, List.of(
                new ProduceRequest.TopicData("orders", List.of(new ProduceRequest.PartitionData(0, rawBatch)))));

        ProduceRequest decoded = roundTrip(request::writeTo, ProduceRequest::readFrom);
        RecordBatch batch = RecordBatch.readFrom(
                ByteBuffer.wrap(decoded.topics().getFirst().partitions().getFirst().records()));

        assertThat(new String(batch.records().getFirst().value(), StandardCharsets.UTF_8))
                .isEqualTo("still-valid");
    }

    @Test
    void roundTripsProduceResponse() {
        ProduceResponse response = new ProduceResponse(List.of(
                new ProduceResponse.TopicResponse("orders", List.of(
                        new ProduceResponse.PartitionResponse(0, ErrorCode.NONE, 42L, 1234L, 10L),
                        new ProduceResponse.PartitionResponse(1, ErrorCode.NOT_ENOUGH_REPLICAS, -1L, -1L, 0L)))), 0);

        ProduceResponse decoded = roundTrip(response::writeTo, ProduceResponse::readFrom);

        assertThat(decoded.throttleTimeMs()).isZero();
        List<ProduceResponse.PartitionResponse> partitions = decoded.topics().getFirst().partitions();
        assertThat(partitions.getFirst().errorCode()).isEqualTo(ErrorCode.NONE);
        assertThat(partitions.getFirst().baseOffset()).isEqualTo(42L);
        assertThat(partitions.getLast().errorCode()).isEqualTo(ErrorCode.NOT_ENOUGH_REPLICAS);
    }

    @Test
    void roundTripsFetchRequest() {
        FetchRequest request = new FetchRequest(FetchRequest.CONSUMER_REPLICA_ID, 500, 1024, 1_048_576, (byte) 1,
                List.of(new FetchRequest.TopicData("orders",
                        List.of(new FetchRequest.PartitionData(2, 7, 100L, 0L, 65_536)))));

        FetchRequest decoded = roundTrip(request::writeTo, FetchRequest::readFrom);

        assertThat(decoded.isFromFollower()).isFalse();
        assertThat(decoded.maxWaitMs()).isEqualTo(500);
        assertThat(decoded.minBytes()).isEqualTo(1024);
        assertThat(decoded.isolationLevel()).isEqualTo((byte) 1);
        FetchRequest.PartitionData partition = decoded.topics().getFirst().partitions().getFirst();
        assertThat(partition.index()).isEqualTo(2);
        assertThat(partition.currentLeaderEpoch()).isEqualTo(7);
        assertThat(partition.fetchOffset()).isEqualTo(100L);
        assertThat(partition.partitionMaxBytes()).isEqualTo(65_536);
    }

    @Test
    void followerFetchIsDistinguishedByReplicaId() {
        FetchRequest follower = new FetchRequest(3, 500, 1, Integer.MAX_VALUE, (byte) 0, List.of());

        assertThat(roundTrip(follower::writeTo, FetchRequest::readFrom).isFromFollower()).isTrue();
    }

    @Test
    void roundTripsFetchResponseWithConcatenatedBatches() {
        ByteBuf batches = Unpooled.buffer();
        batches.writeBytes(rawBatch("one"));
        batches.writeBytes(rawBatch("two"));
        byte[] records = new byte[batches.readableBytes()];
        batches.readBytes(records);

        FetchResponse response = new FetchResponse(0, List.of(
                new FetchResponse.TopicResponse("orders", List.of(
                        new FetchResponse.PartitionResponse(0, ErrorCode.NONE, 99L, 5L, records),
                        new FetchResponse.PartitionResponse(1, ErrorCode.OFFSET_OUT_OF_RANGE, -1L, 0L, null)))));

        FetchResponse decoded = roundTrip(response::writeTo, FetchResponse::readFrom);

        FetchResponse.PartitionResponse first = decoded.topics().getFirst().partitions().getFirst();
        assertThat(first.highWatermark()).isEqualTo(99L);
        assertThat(first.logStartOffset()).isEqualTo(5L);
        ByteBuffer buffer = ByteBuffer.wrap(first.records());
        assertThat(RecordBatch.readFrom(buffer).recordCount()).isEqualTo(1);
        assertThat(RecordBatch.readFrom(buffer).recordCount()).isEqualTo(1);
        assertThat(decoded.topics().getFirst().partitions().getLast().records()).isNull();
    }

    @Test
    void roundTripsApiVersions() {
        ApiVersionsResponse response = ApiVersionsResponse.supporting(
                ApiKey.PRODUCE, ApiKey.FETCH, ApiKey.API_VERSIONS);

        ApiVersionsRequest decodedRequest = roundTrip(
                new ApiVersionsRequest("mq-client", "0.0.1")::writeTo, ApiVersionsRequest::readFrom);
        ApiVersionsResponse decoded = roundTrip(response::writeTo, ApiVersionsResponse::readFrom);

        assertThat(decodedRequest.clientSoftwareName()).isEqualTo("mq-client");
        assertThat(decodedRequest.clientSoftwareVersion()).isEqualTo("0.0.1");
        assertThat(decoded.errorCode()).isEqualTo(ErrorCode.NONE);
        assertThat(decoded.apiVersions()).extracting(ApiVersionsResponse.ApiVersion::apiKey)
                .containsExactly(ApiKey.PRODUCE, ApiKey.FETCH, ApiKey.API_VERSIONS);
    }

    @Test
    void emptyTopicListsSurviveTheRoundTrip() {
        ProduceRequest request = new ProduceRequest((short) 0, 100, "tx", List.of());

        ProduceRequest decoded = roundTrip(request::writeTo, ProduceRequest::readFrom);

        assertThat(decoded.topics()).isEmpty();
        assertThat(decoded.transactionalId()).isEqualTo("tx");
    }

    private static byte[] rawBatch(String value) {
        RecordBatch batch = new RecordBatchBuilder()
                .append(1_000L, null, value.getBytes(StandardCharsets.UTF_8))
                .build();
        ByteBuffer buffer = batch.toBuffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static <T> T roundTrip(Consumer<ByteBuf> writer, Function<ByteBuf, T> reader) {
        ByteBuf buf = Unpooled.buffer();
        writer.accept(buf);
        T decoded = reader.apply(buf);
        assertThat(buf.readableBytes()).isZero();
        return decoded;
    }
}
