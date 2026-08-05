package com.github.nicolasholanda.mq.broker.log;

import java.io.File;
import java.io.IOException;

public final class OffsetIndex extends AbstractIndex {

    public static final String INDEX_SUFFIX = ".index";
    public static final int ENTRY_SIZE = 8;

    public OffsetIndex(File file, long baseOffset, int maxIndexSize) throws IOException {
        super(file, baseOffset, maxIndexSize, ENTRY_SIZE);
    }

    public static OffsetIndex open(File dir, long baseOffset, int maxIndexSize) throws IOException {
        return new OffsetIndex(new File(dir, LogSegment.fileName(baseOffset, INDEX_SUFFIX)), baseOffset, maxIndexSize);
    }

    @Override
    protected boolean isEmptySlot(int slot) {
        return relativeOffsetAt(slot) == 0 && positionAt(slot) == 0;
    }

    public void append(long offset, int position) {
        if (isFull()) {
            throw new IllegalStateException("Offset index is full: " + file().getName());
        }
        if (!isEmpty() && offset <= lastOffset()) {
            throw new IllegalArgumentException("Offset " + offset + " is not greater than the last indexed offset "
                    + lastOffset());
        }
        long relative = offset - baseOffset();
        if (relative <= 0 || relative > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Offset " + offset + " cannot be indexed in segment " + baseOffset());
        }
        int slot = slotPosition(entries());
        buffer().putInt(slot, (int) relative);
        buffer().putInt(slot + Integer.BYTES, position);
        entryAppended();
    }

    public IndexEntry lookup(long targetOffset) {
        if (isEmpty()) {
            return IndexEntry.EMPTY;
        }
        int slot = largestSlotLessThanOrEqual(targetOffset - baseOffset());
        return slot < 0 ? IndexEntry.EMPTY : entryAt(slot);
    }

    private int largestSlotLessThanOrEqual(long relativeTarget) {
        int low = 0;
        int high = entries() - 1;
        int result = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (relativeOffsetAt(mid) <= relativeTarget) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return result;
    }

    public IndexEntry entryAt(int slot) {
        return new IndexEntry(baseOffset() + relativeOffsetAt(slot), positionAt(slot));
    }

    public long lastOffset() {
        return isEmpty() ? baseOffset() : baseOffset() + relativeOffsetAt(entries() - 1);
    }

    public void truncateTo(long offset) {
        int slot = largestSlotLessThanOrEqual(offset - baseOffset() - 1);
        truncateToEntries(slot + 1);
    }

    private int relativeOffsetAt(int slot) {
        return buffer().getInt(slotPosition(slot));
    }

    private int positionAt(int slot) {
        return buffer().getInt(slotPosition(slot) + Integer.BYTES);
    }
}
