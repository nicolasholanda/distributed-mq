package com.github.nicolasholanda.mq.broker.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.nicolasholanda.mq.common.record.RecordBatch;
import com.github.nicolasholanda.mq.common.record.RecordBatchBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogTest {

    private static final LogConfig CONFIG = LogConfig.defaults()
            .withSegmentBytes(4096)
            .withIndexIntervalBytes(256)
            .withMaxIndexSize(8 * 1024);

    @TempDir
    File dir;

    @Test
    void assignsDenseMonotonicOffsets() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 500; i++) {
                LogAppendInfo info = log.append(batch("v" + i), true);
                assertThat(info.baseOffset()).isEqualTo(i);
            }

            assertThat(log.logStartOffset()).isZero();
            assertThat(log.logEndOffset()).isEqualTo(500L);
        }
    }

    @Test
    void readsBackEveryRecordInOrder() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 2000; i++) {
                log.append(batch("payload-" + i), true);
            }
            log.updateHighWatermark(log.logEndOffset());

            List<String> values = readAll(log, 0L);

            assertThat(values).hasSize(2000);
            for (int i = 0; i < 2000; i++) {
                assertThat(values.get(i)).isEqualTo("payload-" + i);
            }
        }
    }

    @Test
    void rollsSegmentsOnSegmentBytes() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 500; i++) {
                log.append(batch("v" + i), true);
            }

            assertThat(log.numberOfSegments()).isGreaterThan(1);
            assertThat(log.segments().firstKey()).isZero();
            for (LogSegment segment : log.segments().values()) {
                assertThat(segment.file().getName()).matches("\\d{20}\\.log");
                assertThat(LogSegment.offsetFromFileName(segment.file().getName()))
                        .isEqualTo(segment.baseOffset());
            }
        }
    }

    @Test
    void rollsSegmentOnTime() throws IOException {
        try (Log log = Log.open(dir, CONFIG.withSegmentMs(0L))) {
            log.append(batch("first"), true);
            log.append(batch("second"), true);

            assertThat(log.numberOfSegments()).isEqualTo(2);
        }
    }

    @Test
    void readFromArbitraryOffsetStartsExactlyThere() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 1000; i++) {
                log.append(batch("v" + i), true);
            }
            log.updateHighWatermark(log.logEndOffset());

            for (long offset : new long[] {0L, 1L, 137L, 511L, 999L}) {
                FetchDataInfo fetch = log.read(offset, Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
                assertThat(fetch.batches().getFirst().baseOffset()).isEqualTo(offset);
                assertThat(value(fetch.batches().getFirst())).isEqualTo("v" + offset);
            }
        }
    }

    @Test
    void readRespectsMaxBytesButNeverReturnsPartialBatch() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 100; i++) {
                log.append(batch("x".repeat(200)), true);
            }

            FetchDataInfo tiny = log.read(0L, 1, IsolationLevel.READ_UNCOMMITTED);
            FetchDataInfo bounded = log.read(0L, 1000, IsolationLevel.READ_UNCOMMITTED);

            assertThat(tiny.batches()).hasSize(1);
            assertThat(tiny.sizeInBytes()).isGreaterThan(1);
            assertThat(bounded.batches()).hasSizeGreaterThan(1).hasSizeLessThan(100);
            assertThat(bounded.sizeInBytes()).isLessThanOrEqualTo(1000);
            assertThat(bounded.sizeInBytes() + bounded.batches().getFirst().sizeInBytes())
                    .isGreaterThan(1000);
        }
    }

    @Test
    void readNeverGoesAboveHighWatermarkWhenReadCommitted() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 50; i++) {
                log.append(batch("v" + i), true);
            }
            log.updateHighWatermark(20L);

            FetchDataInfo fetch = log.read(0L, Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);

            assertThat(fetch.batches()).hasSize(20);
            assertThat(fetch.batches().getLast().lastOffset()).isEqualTo(19L);
            assertThat(fetch.highWatermark()).isEqualTo(20L);
        }
    }

    @Test
    void rejectsOffsetsOutOfRange() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            log.append(batch("only"), true);

            assertThatThrownBy(() -> log.read(99L, 1024, IsolationLevel.READ_UNCOMMITTED))
                    .isInstanceOf(OffsetOutOfRangeException.class);
        }
    }

    @Test
    void rejectsBatchesLargerThanMaxMessageBytes() throws IOException {
        try (Log log = Log.open(dir, CONFIG.withMaxMessageBytes(128))) {
            assertThatThrownBy(() -> log.append(batch("y".repeat(500)), true))
                    .isInstanceOf(RecordTooLargeException.class);
            assertThat(log.logEndOffset()).isZero();
        }
    }

    @Test
    void rejectsNonContiguousFollowerAppend() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            log.append(batch("a"), true);

            RecordBatch gap = new RecordBatchBuilder().baseOffset(50L)
                    .append(1L, null, "b".getBytes(StandardCharsets.UTF_8)).build();

            assertThatThrownBy(() -> log.append(gap, false)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void followerAppendKeepsIncomingOffsets() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            RecordBatch replicated = new RecordBatchBuilder().baseOffset(0L)
                    .append(1L, null, "a".getBytes(StandardCharsets.UTF_8))
                    .append(2L, null, "b".getBytes(StandardCharsets.UTF_8))
                    .build();

            log.append(replicated, false);

            assertThat(log.logEndOffset()).isEqualTo(2L);
        }
    }

    @Test
    void truncateDropsSegmentsAndResetsEndOffset() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 500; i++) {
                log.append(batch("v" + i), true);
            }
            int segmentsBefore = log.numberOfSegments();

            log.truncateTo(10L);

            assertThat(segmentsBefore).isGreaterThan(1);
            assertThat(log.numberOfSegments()).isEqualTo(1);
            assertThat(log.logEndOffset()).isEqualTo(10L);
            assertThat(readAll(log, 0L)).hasSize(10);
        }
    }

    @Test
    void reopenRestoresOffsetsAndSegments() throws IOException {
        int segments;
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 400; i++) {
                log.append(batch("v" + i), true);
            }
            segments = log.numberOfSegments();
            log.flush();
        }

        try (Log reopened = Log.open(dir, CONFIG)) {
            assertThat(reopened.numberOfSegments()).isEqualTo(segments);
            assertThat(reopened.logEndOffset()).isEqualTo(400L);
            assertThat(reopened.logStartOffset()).isZero();
            assertThat(readAll(reopened, 0L)).hasSize(400);
        }
    }

    @Test
    void resolvesOffsetForTimestampAcrossSegments() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 400; i++) {
                log.append(new RecordBatchBuilder()
                        .append(100_000L + i * 5L, null, ("v" + i).getBytes(StandardCharsets.UTF_8))
                        .build(), true);
            }

            assertThat(log.offsetForTimestamp(100_000L).offset()).isZero();
            assertThat(log.offsetForTimestamp(101_000L).offset()).isEqualTo(200L);
            assertThat(log.offsetForTimestamp(999_999L)).isEqualTo(OffsetAndTimestamp.NONE);
        }
    }

    @Test
    void randomAppendReadRollSequenceKeepsOffsetsAligned() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            Random random = new Random(42);
            long expectedEndOffset = 0;
            for (int round = 0; round < 200; round++) {
                int records = 1 + random.nextInt(4);
                RecordBatchBuilder builder = new RecordBatchBuilder();
                for (int i = 0; i < records; i++) {
                    builder.append(1_000L + round, null, ("r" + round + "-" + i).getBytes(StandardCharsets.UTF_8));
                }
                log.append(builder.build(), true);
                expectedEndOffset += records;
                assertThat(log.logEndOffset()).isEqualTo(expectedEndOffset);

                long target = (long) random.nextInt((int) expectedEndOffset);
                FetchDataInfo fetch = log.read(target, 4096, IsolationLevel.READ_UNCOMMITTED);
                assertThat(fetch.batches().getFirst().baseOffset()).isLessThanOrEqualTo(target);
                assertThat(fetch.batches().getFirst().lastOffset()).isGreaterThanOrEqualTo(target);
            }
        }
    }

    private static RecordBatch batch(String value) {
        return new RecordBatchBuilder()
                .append(System.currentTimeMillis(), null, value.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    private static String value(RecordBatch batch) {
        return new String(batch.records().getFirst().value(), StandardCharsets.UTF_8);
    }

    private static List<String> readAll(Log log, long startOffset) throws IOException {
        List<String> values = new ArrayList<>();
        long offset = startOffset;
        while (offset < log.logEndOffset()) {
            FetchDataInfo fetch = log.read(offset, 8192, IsolationLevel.READ_UNCOMMITTED);
            if (fetch.isEmpty()) {
                break;
            }
            for (RecordBatch batch : fetch.batches()) {
                values.add(value(batch));
            }
            offset = fetch.batches().getLast().lastOffset() + 1;
        }
        return values;
    }
}
