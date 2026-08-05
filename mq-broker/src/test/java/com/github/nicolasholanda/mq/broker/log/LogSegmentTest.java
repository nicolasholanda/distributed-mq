package com.github.nicolasholanda.mq.broker.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.nicolasholanda.mq.common.record.RecordBatch;
import com.github.nicolasholanda.mq.common.record.RecordBatchBuilder;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogSegmentTest {

    @TempDir
    File dir;

    @Test
    void fileNameIsBaseOffsetPaddedToTwentyDigits() {
        assertThat(LogSegment.fileName(0L, ".log")).isEqualTo("00000000000000000000.log");
        assertThat(LogSegment.fileName(16384L, ".log")).isEqualTo("00000000000000016384.log");
        assertThat(LogSegment.offsetFromFileName("00000000000000016384.log")).isEqualTo(16384L);
    }

    @Test
    void appendsAndReadsBackEveryRecord() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L)) {
            for (int i = 0; i < 200; i++) {
                segment.append(batch(i, "v" + i));
            }

            List<RecordBatch> batches = segment.readFrom(0L, Integer.MAX_VALUE);

            assertThat(batches).hasSize(200);
            assertThat(segment.nextOffset()).isEqualTo(200L);
            for (int i = 0; i < 200; i++) {
                assertThat(batches.get(i).baseOffset()).isEqualTo(i);
                assertThat(value(batches.get(i))).isEqualTo("v" + i);
            }
        }
    }

    @Test
    void readsFromArbitraryOffset() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L)) {
            for (int i = 0; i < 50; i++) {
                segment.append(batch(i, "v" + i));
            }

            List<RecordBatch> batches = segment.readFrom(37L, Integer.MAX_VALUE);

            assertThat(batches).hasSize(13);
            assertThat(batches.getFirst().baseOffset()).isEqualTo(37L);
        }
    }

    @Test
    void neverReturnsPartialBatchAndAlwaysReturnsAtLeastOne() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L)) {
            segment.append(batch(0, "a".repeat(500)));
            segment.append(batch(1, "b".repeat(500)));

            List<RecordBatch> batches = segment.readFrom(0L, 1);

            assertThat(batches).hasSize(1);
            assertThat(value(batches.getFirst())).isEqualTo("a".repeat(500));
        }
    }

    @Test
    void tracksSizeAndMaxTimestamp() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L)) {
            segment.append(new RecordBatchBuilder().baseOffset(0L).append(100L, null, new byte[8]).build());
            segment.append(new RecordBatchBuilder().baseOffset(1L).append(300L, null, new byte[8]).build());

            assertThat(segment.maxTimestamp()).isEqualTo(300L);
            assertThat(segment.offsetOfMaxTimestamp()).isEqualTo(1L);
            assertThat(segment.sizeInBytes()).isEqualTo(2 * (RecordBatch.HEADER_SIZE + 37));
            assertThat(segment.baseOffset()).isZero();
            assertThat(segment.isEmpty()).isFalse();
        }
    }

    @Test
    void rejectsOutOfOrderAppend() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L)) {
            segment.append(batch(5, "v"));

            assertThatThrownBy(() -> segment.append(batch(3, "v")))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void truncateDropsBatchesAtOrAfterOffset() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L)) {
            for (int i = 0; i < 10; i++) {
                segment.append(batch(i, "v" + i));
            }

            segment.truncateTo(4L);

            assertThat(segment.nextOffset()).isEqualTo(4L);
            assertThat(segment.readFrom(0L, Integer.MAX_VALUE)).hasSize(4);
        }
    }

    @Test
    void reopeningDiscardsTrailingPartialBatch() throws IOException {
        int fullSize;
        try (LogSegment segment = LogSegment.open(dir, 0L)) {
            for (int i = 0; i < 5; i++) {
                segment.append(batch(i, "v" + i));
            }
            fullSize = segment.sizeInBytes();
        }

        File logFile = new File(dir, LogSegment.fileName(0L, LogSegment.LOG_SUFFIX));
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw")) {
            raf.setLength(fullSize - 7);
        }

        try (LogSegment segment = LogSegment.open(dir, 0L)) {
            assertThat(segment.readFrom(0L, Integer.MAX_VALUE)).hasSize(4);
            assertThat(segment.nextOffset()).isEqualTo(4L);
        }
    }

    @Test
    void reopeningRestoresStateFromDisk() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L)) {
            for (int i = 0; i < 3; i++) {
                segment.append(batch(i, "v" + i));
            }
        }

        try (LogSegment segment = LogSegment.open(dir, 0L)) {
            assertThat(segment.nextOffset()).isEqualTo(3L);
            assertThat(segment.readFrom(0L, Integer.MAX_VALUE)).hasSize(3);
        }
    }

    private static RecordBatch batch(long baseOffset, String value) {
        return new RecordBatchBuilder()
                .baseOffset(baseOffset)
                .append(1_000L + baseOffset, null, value.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    private static String value(RecordBatch batch) {
        return new String(batch.records().getFirst().value(), StandardCharsets.UTF_8);
    }
}
