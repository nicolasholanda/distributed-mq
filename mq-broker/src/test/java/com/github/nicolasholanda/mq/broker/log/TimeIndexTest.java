package com.github.nicolasholanda.mq.broker.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.nicolasholanda.mq.common.record.RecordBatchBuilder;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TimeIndexTest {

    private static final int MAX_INDEX_SIZE = 12 * 1024;

    @TempDir
    File dir;

    @Test
    void entriesAreTwelveBytesWide() throws IOException {
        try (TimeIndex index = TimeIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            index.append(1_000L, 5L);
            index.append(2_000L, 9L);

            assertThat(TimeIndex.ENTRY_SIZE).isEqualTo(12);
            assertThat(index.entries()).isEqualTo(2);
            assertThat(index.sizeInBytes()).isEqualTo(24);
            assertThat(index.file().getName()).isEqualTo("00000000000000000000.timeindex");
        }
    }

    @Test
    void lookupReturnsLargestEntryNotAboveTimestamp() throws IOException {
        try (TimeIndex index = TimeIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            for (int i = 1; i <= 50; i++) {
                index.append(i * 100L, i * 10L);
            }

            assertThat(index.lookup(1_050L).offset()).isEqualTo(100L);
            assertThat(index.lookup(1_000L).offset()).isEqualTo(100L);
            assertThat(index.lookup(50L)).isEqualTo(IndexEntry.EMPTY);
            assertThat(index.lookup(999_999L).offset()).isEqualTo(500L);
        }
    }

    @Test
    void rejectsDecreasingTimestamps() throws IOException {
        try (TimeIndex index = TimeIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            index.append(5_000L, 10L);

            assertThatThrownBy(() -> index.append(4_999L, 20L)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void skipsDuplicateTimestamps() throws IOException {
        try (TimeIndex index = TimeIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            index.append(5_000L, 10L);
            index.append(5_000L, 20L);

            assertThat(index.entries()).isEqualTo(1);
            assertThat(index.lastOffset()).isEqualTo(10L);
        }
    }

    @Test
    void truncateDropsEntriesAtOrAboveOffset() throws IOException {
        try (TimeIndex index = TimeIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            for (int i = 1; i <= 10; i++) {
                index.append(i * 100L, i * 10L);
            }

            index.truncateTo(55L);

            assertThat(index.entries()).isEqualTo(5);
            assertThat(index.lastOffset()).isEqualTo(50L);
            assertThat(index.lastTimestamp()).isEqualTo(500L);
        }
    }

    @Test
    void survivesReopen() throws IOException {
        try (TimeIndex index = TimeIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            for (int i = 1; i <= 12; i++) {
                index.append(i * 250L, i * 3L);
            }
            index.flush();
        }

        try (TimeIndex reopened = TimeIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            assertThat(reopened.entries()).isEqualTo(12);
            assertThat(reopened.lookup(2_500L).offset()).isEqualTo(30L);
        }
    }

    @Test
    void segmentResolvesOffsetForTimestamp() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L, 512, MAX_INDEX_SIZE)) {
            for (int i = 0; i < 300; i++) {
                segment.append(new RecordBatchBuilder()
                        .baseOffset(i)
                        .append(10_000L + i * 10L, null, new byte[64])
                        .build());
            }

            assertThat(segment.offsetForTimestamp(10_000L)).isEqualTo(new OffsetAndTimestamp(0L, 10_000L));
            assertThat(segment.offsetForTimestamp(11_500L)).isEqualTo(new OffsetAndTimestamp(150L, 11_500L));
            assertThat(segment.offsetForTimestamp(11_501L)).isEqualTo(new OffsetAndTimestamp(151L, 11_510L));
            assertThat(segment.offsetForTimestamp(12_990L)).isEqualTo(new OffsetAndTimestamp(299L, 12_990L));
        }
    }

    @Test
    void segmentReturnsNoneForTimestampAboveMax() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L, 512, MAX_INDEX_SIZE)) {
            segment.append(new RecordBatchBuilder().baseOffset(0L).append(1_000L, null, new byte[8]).build());

            assertThat(segment.offsetForTimestamp(2_000L)).isEqualTo(OffsetAndTimestamp.NONE);
        }
    }

    @Test
    void segmentRebuildsTimeIndexWhenFileIsDeleted() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L, 512, MAX_INDEX_SIZE)) {
            for (int i = 0; i < 300; i++) {
                segment.append(new RecordBatchBuilder()
                        .baseOffset(i)
                        .append(10_000L + i * 10L, null, new byte[64])
                        .build());
            }
        }

        assertThat(new File(dir, "00000000000000000000.timeindex").delete()).isTrue();

        try (LogSegment reopened = LogSegment.open(dir, 0L, 512, MAX_INDEX_SIZE)) {
            assertThat(reopened.timeIndex().entries()).isPositive();
            assertThat(reopened.offsetForTimestamp(11_500L)).isEqualTo(new OffsetAndTimestamp(150L, 11_500L));
        }
    }

    @Test
    void timeIndexIsMonotonicAcrossSegmentAppends() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L, 256, MAX_INDEX_SIZE)) {
            for (int i = 0; i < 500; i++) {
                segment.append(new RecordBatchBuilder()
                        .baseOffset(i)
                        .append(50_000L + i, null, new byte[32])
                        .build());
            }

            TimeIndex index = segment.timeIndex();
            for (int slot = 1; slot < index.entries(); slot++) {
                assertThat(index.timestampAt(slot)).isGreaterThan(index.timestampAt(slot - 1));
                assertThat(index.offsetAt(slot)).isGreaterThan(index.offsetAt(slot - 1));
            }
        }
    }
}
