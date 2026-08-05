package com.github.nicolasholanda.mq.broker.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.nicolasholanda.mq.common.record.RecordBatchBuilder;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OffsetIndexTest {

    private static final int MAX_INDEX_SIZE = 8 * 1024;

    @TempDir
    File dir;

    @Test
    void indexFileIsNamedAfterBaseOffset() throws IOException {
        try (OffsetIndex index = OffsetIndex.open(dir, 16384L, MAX_INDEX_SIZE)) {
            assertThat(index.file().getName()).isEqualTo("00000000000000016384.index");
        }
    }

    @Test
    void entriesAreEightBytesWide() throws IOException {
        try (OffsetIndex index = OffsetIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            index.append(10L, 100);
            index.append(20L, 200);

            assertThat(OffsetIndex.ENTRY_SIZE).isEqualTo(8);
            assertThat(index.sizeInBytes()).isEqualTo(16);
            assertThat(index.entries()).isEqualTo(2);
        }
    }

    @Test
    void lookupReturnsLargestEntryNotAboveTarget() throws IOException {
        try (OffsetIndex index = OffsetIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            for (int i = 1; i <= 100; i++) {
                index.append(i * 10L, i * 4096);
            }

            assertThat(index.lookup(355L)).isEqualTo(new IndexEntry(350L, 35 * 4096));
            assertThat(index.lookup(350L)).isEqualTo(new IndexEntry(350L, 35 * 4096));
            assertThat(index.lookup(1000L)).isEqualTo(new IndexEntry(1000L, 100 * 4096));
            assertThat(index.lookup(99999L)).isEqualTo(new IndexEntry(1000L, 100 * 4096));
        }
    }

    @Test
    void lookupBelowFirstEntryReturnsEmpty() throws IOException {
        try (OffsetIndex index = OffsetIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            index.append(50L, 4096);

            assertThat(index.lookup(10L)).isEqualTo(IndexEntry.EMPTY);
        }
    }

    @Test
    void emptyIndexAlwaysReturnsEmptyEntry() throws IOException {
        try (OffsetIndex index = OffsetIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            assertThat(index.isEmpty()).isTrue();
            assertThat(index.lookup(1L)).isEqualTo(IndexEntry.EMPTY);
        }
    }

    @Test
    void rejectsNonMonotonicAppend() throws IOException {
        try (OffsetIndex index = OffsetIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            index.append(10L, 0);

            assertThatThrownBy(() -> index.append(5L, 10)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> index.append(10L, 10)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsAppendWhenFull() throws IOException {
        try (OffsetIndex index = OffsetIndex.open(dir, 0L, OffsetIndex.ENTRY_SIZE * 2)) {
            index.append(1L, 0);
            index.append(2L, 8);

            assertThat(index.isFull()).isTrue();
            assertThatThrownBy(() -> index.append(3L, 16)).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void truncateDropsEntriesAtOrAboveOffset() throws IOException {
        try (OffsetIndex index = OffsetIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            for (int i = 1; i <= 10; i++) {
                index.append(i * 10L, i * 100);
            }

            index.truncateTo(55L);

            assertThat(index.entries()).isEqualTo(5);
            assertThat(index.lastOffset()).isEqualTo(50L);
            assertThat(index.lookup(90L)).isEqualTo(new IndexEntry(50L, 500));
        }
    }

    @Test
    void survivesReopen() throws IOException {
        try (OffsetIndex index = OffsetIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            for (int i = 1; i <= 20; i++) {
                index.append(i * 7L, i * 64);
            }
            index.flush();
        }

        try (OffsetIndex reopened = OffsetIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            assertThat(reopened.entries()).isEqualTo(20);
            assertThat(reopened.lookup(70L)).isEqualTo(new IndexEntry(70L, 640));
        }
    }

    @Test
    void trimShrinksFileToValidSize() throws IOException {
        File indexFile;
        try (OffsetIndex index = OffsetIndex.open(dir, 0L, MAX_INDEX_SIZE)) {
            index.append(1L, 0);
            index.append(2L, 32);
            index.trimToValidSize();
            indexFile = index.file();
            assertThat(index.entries()).isEqualTo(2);
        }

        assertThat(indexFile).hasSize(16);
    }

    @Test
    void segmentIndexesSparselyAndReadsFromArbitraryOffset() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L, 512, MAX_INDEX_SIZE)) {
            for (int i = 0; i < 300; i++) {
                segment.append(new RecordBatchBuilder()
                        .baseOffset(i)
                        .append(1000L + i, null, new byte[64])
                        .build());
            }

            assertThat(segment.offsetIndex().entries()).isBetween(30, 80);
            assertThat(segment.readFrom(250L, Integer.MAX_VALUE)).hasSize(50);
            assertThat(segment.readFrom(250L, Integer.MAX_VALUE).getFirst().baseOffset()).isEqualTo(250L);
        }
    }

    @Test
    void segmentRebuildsIndexWhenFileIsDeleted() throws IOException {
        try (LogSegment segment = LogSegment.open(dir, 0L, 512, MAX_INDEX_SIZE)) {
            for (int i = 0; i < 300; i++) {
                segment.append(new RecordBatchBuilder()
                        .baseOffset(i)
                        .append(1000L + i, null, new byte[64])
                        .build());
            }
        }

        assertThat(new File(dir, "00000000000000000000.index").delete()).isTrue();

        try (LogSegment reopened = LogSegment.open(dir, 0L, 512, MAX_INDEX_SIZE)) {
            assertThat(reopened.offsetIndex().entries()).isPositive();
            assertThat(reopened.readFrom(275L, Integer.MAX_VALUE)).hasSize(25);
            assertThat(reopened.nextOffset()).isEqualTo(300L);
        }
    }
}
