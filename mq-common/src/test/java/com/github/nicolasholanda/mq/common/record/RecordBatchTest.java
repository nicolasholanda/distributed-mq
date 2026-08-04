package com.github.nicolasholanda.mq.common.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class RecordBatchTest {

    private static String asString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Test
    void batchHeaderIsSixtyOneBytes() {
        RecordBatch batch = new RecordBatchBuilder()
                .append(1000L, null, null)
                .build();

        Record onlyRecord = batch.records().getFirst();
        assertThat(batch.sizeInBytes() - onlyRecord.sizeInBytes()).isEqualTo(61);
        assertThat(RecordBatch.HEADER_SIZE).isEqualTo(61);
    }

    @Test
    void roundTripsSingleRecord() {
        RecordBatch batch = new RecordBatchBuilder()
                .baseOffset(42L)
                .partitionLeaderEpoch(7)
                .append(1000L, "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8))
                .build();

        RecordBatch decoded = RecordBatch.readFrom(batch.toBuffer());

        assertThat(decoded.baseOffset()).isEqualTo(42L);
        assertThat(decoded.partitionLeaderEpoch()).isEqualTo(7);
        assertThat(decoded.recordCount()).isEqualTo(1);
        assertThat(asString(decoded.records().getFirst().key())).isEqualTo("key");
        assertThat(asString(decoded.records().getFirst().value())).isEqualTo("value");
    }

    @Test
    void roundTripsManyRecordsWithNullKeysAndHeaders() {
        RecordBatchBuilder builder = new RecordBatchBuilder().baseOffset(100L);
        for (int i = 0; i < 500; i++) {
            builder.append(2000L + i,
                    i % 3 == 0 ? null : ("k" + i).getBytes(StandardCharsets.UTF_8),
                    ("v" + i).getBytes(StandardCharsets.UTF_8),
                    List.of(new RecordHeader("trace", ("t" + i).getBytes(StandardCharsets.UTF_8))));
        }
        RecordBatch batch = builder.build();

        RecordBatch decoded = RecordBatch.readFrom(batch.toBuffer());

        assertThat(decoded.recordCount()).isEqualTo(500);
        assertThat(decoded.lastOffset()).isEqualTo(599L);
        assertThat(decoded.baseTimestamp()).isEqualTo(2000L);
        assertThat(decoded.maxTimestamp()).isEqualTo(2499L);
        for (int i = 0; i < 500; i++) {
            Record record = decoded.records().get(i);
            assertThat(record.offsetDelta()).isEqualTo(i);
            assertThat(record.timestampDelta()).isEqualTo(i);
            assertThat(asString(record.value())).isEqualTo("v" + i);
            assertThat(record.headers()).singleElement()
                    .satisfies(header -> assertThat(header.key()).isEqualTo("trace"));
            if (i % 3 == 0) {
                assertThat(record.key()).isNull();
            } else {
                assertThat(asString(record.key())).isEqualTo("k" + i);
            }
        }
    }

    @Test
    void reportsAttributesAndProducerState() {
        RecordBatch batch = new RecordBatchBuilder()
                .compressionType(CompressionType.NONE)
                .producer(99L, (short) 3, 12)
                .append(1L, null, new byte[] {1})
                .build();

        RecordBatch decoded = RecordBatch.readFrom(batch.toBuffer());

        assertThat(decoded.compressionType()).isEqualTo(CompressionType.NONE);
        assertThat(decoded.isControlBatch()).isFalse();
        assertThat(decoded.isIdempotent()).isTrue();
        assertThat(decoded.producerId()).isEqualTo(99L);
        assertThat(decoded.producerEpoch()).isEqualTo((short) 3);
        assertThat(decoded.baseSequence()).isEqualTo(12);
    }

    @Test
    void nonIdempotentBatchUsesSentinelProducerFields() {
        RecordBatch decoded = RecordBatch.readFrom(
                new RecordBatchBuilder().append(1L, null, new byte[] {1}).build().toBuffer());

        assertThat(decoded.isIdempotent()).isFalse();
        assertThat(decoded.producerId()).isEqualTo(-1L);
        assertThat(decoded.producerEpoch()).isEqualTo((short) -1);
        assertThat(decoded.baseSequence()).isEqualTo(-1);
    }

    @Test
    void rejectsCorruptedPayload() {
        RecordBatch batch = new RecordBatchBuilder()
                .append(1L, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8))
                .build();
        ByteBuffer buffer = batch.toBuffer();
        buffer.put(buffer.limit() - 1, (byte) (buffer.get(buffer.limit() - 1) ^ 0xFF));

        assertThatThrownBy(() -> RecordBatch.readFrom(buffer))
                .isInstanceOf(CorruptRecordException.class)
                .hasMessageContaining("CRC mismatch");
    }

    @Test
    void rejectsTruncatedBatch() {
        RecordBatch batch = new RecordBatchBuilder()
                .append(1L, null, new byte[64])
                .build();
        ByteBuffer buffer = batch.toBuffer();
        buffer.limit(buffer.limit() - 10);

        assertThatThrownBy(() -> RecordBatch.readFrom(buffer))
                .isInstanceOf(CorruptRecordException.class)
                .hasMessageContaining("truncated");
    }

    @Test
    void peekBatchLengthMatchesEncodedSize() {
        RecordBatch batch = new RecordBatchBuilder().append(1L, null, new byte[32]).build();
        ByteBuffer buffer = batch.toBuffer();

        assertThat(RecordBatch.peekBatchLength(buffer, 0)).isEqualTo(batch.sizeInBytes());
    }

    @Test
    void readsConcatenatedBatchesSequentially() {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        Random random = new Random(7);
        for (int i = 0; i < 5; i++) {
            byte[] value = new byte[random.nextInt(50) + 1];
            random.nextBytes(value);
            new RecordBatchBuilder().baseOffset(i * 10L).append(500L + i, null, value).build().writeTo(buffer);
        }
        buffer.flip();

        for (int i = 0; i < 5; i++) {
            assertThat(RecordBatch.readFrom(buffer).baseOffset()).isEqualTo(i * 10L);
        }
        assertThat(buffer.hasRemaining()).isFalse();
    }

    @Test
    void withBaseOffsetPreservesCrcValidity() {
        RecordBatch batch = new RecordBatchBuilder().append(1L, null, new byte[] {9}).build();

        RecordBatch reassigned = batch.withBaseOffset(1234L);
        RecordBatch decoded = RecordBatch.readFrom(reassigned.toBuffer());

        assertThat(decoded.baseOffset()).isEqualTo(1234L);
    }
}
