package com.github.nicolasholanda.mq.broker.log;

import java.io.File;
import java.io.IOException;

public final class TimeIndex extends AbstractIndex {

    public static final String TIME_INDEX_SUFFIX = ".timeindex";
    public static final int ENTRY_SIZE = 12;

    public TimeIndex(File file, long baseOffset, int maxIndexSize) throws IOException {
        super(file, baseOffset, maxIndexSize, ENTRY_SIZE);
    }

    public static TimeIndex open(File dir, long baseOffset, int maxIndexSize) throws IOException {
        return new TimeIndex(new File(dir, LogSegment.fileName(baseOffset, TIME_INDEX_SUFFIX)),
                baseOffset, maxIndexSize);
    }

    @Override
    protected boolean isEmptySlot(int slot) {
        return timestampAt(slot) == 0 && relativeOffsetAt(slot) == 0;
    }

    public void append(long timestamp, long offset) {
        if (isFull()) {
            throw new IllegalStateException("Time index is full: " + file().getName());
        }
        if (!isEmpty() && timestamp < lastTimestamp()) {
            throw new IllegalArgumentException("Timestamp " + timestamp
                    + " is smaller than the last indexed timestamp " + lastTimestamp());
        }
        if (!isEmpty() && timestamp == lastTimestamp()) {
            return;
        }
        long relative = offset - baseOffset();
        if (relative <= 0 || relative > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Offset " + offset + " cannot be indexed in segment " + baseOffset());
        }
        int slot = slotPosition(entries());
        buffer().putLong(slot, timestamp);
        buffer().putInt(slot + Long.BYTES, (int) relative);
        entryAppended();
    }

    public IndexEntry lookup(long targetTimestamp) {
        int slot = largestSlotNotAbove(targetTimestamp);
        return slot < 0 ? IndexEntry.EMPTY : entryAt(slot);
    }

    private int largestSlotNotAbove(long targetTimestamp) {
        int low = 0;
        int high = entries() - 1;
        int result = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (timestampAt(mid) <= targetTimestamp) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return result;
    }

    public IndexEntry entryAt(int slot) {
        return new IndexEntry(baseOffset() + relativeOffsetAt(slot), 0);
    }

    public long timestampAt(int slot) {
        return buffer().getLong(slotPosition(slot));
    }

    public long offsetAt(int slot) {
        return baseOffset() + relativeOffsetAt(slot);
    }

    public long lastTimestamp() {
        return isEmpty() ? -1L : timestampAt(entries() - 1);
    }

    public long lastOffset() {
        return isEmpty() ? baseOffset() : offsetAt(entries() - 1);
    }

    public void truncateTo(long offset) {
        int kept = 0;
        for (int slot = 0; slot < entries(); slot++) {
            if (offsetAt(slot) >= offset) {
                break;
            }
            kept++;
        }
        truncateToEntries(kept);
    }

    private int relativeOffsetAt(int slot) {
        return buffer().getInt(slotPosition(slot) + Long.BYTES);
    }
}
