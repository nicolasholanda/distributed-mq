package com.github.nicolasholanda.mq.broker.log;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public abstract class AbstractIndex implements Closeable {

    protected final File file;
    protected final long baseOffset;
    protected final int entrySize;

    private FileChannel channel;
    private Arena arena;
    private MemorySegment segment;
    private ByteBuffer buffer;
    private int entries;

    protected AbstractIndex(File file, long baseOffset, int maxIndexSize, int entrySize) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        this.entrySize = entrySize;
        boolean created = !file.exists() || file.length() == 0;
        this.channel = openChannel();
        long size = created ? (long) (maxIndexSize / entrySize) * entrySize : channel.size();
        map(size);
        this.entries = created ? 0 : countEntries();
    }

    private FileChannel openChannel() throws IOException {
        return FileChannel.open(file.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    private void map(long size) throws IOException {
        arena = Arena.ofShared();
        segment = channel.map(FileChannel.MapMode.READ_WRITE, 0, size, arena);
        buffer = segment.asByteBuffer().order(ByteOrder.BIG_ENDIAN);
    }

    private void unmap() {
        if (arena != null) {
            segment.force();
            arena.close();
            arena = null;
            segment = null;
            buffer = null;
        }
    }

    private int countEntries() {
        int capacity = capacity();
        int low = 0;
        int high = capacity;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (isEmptySlot(mid)) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

    protected abstract boolean isEmptySlot(int slot);

    protected ByteBuffer buffer() {
        return buffer;
    }

    protected int slotPosition(int slot) {
        return slot * entrySize;
    }

    protected void entryAppended() {
        entries++;
    }

    public int entries() {
        return entries;
    }

    public int capacity() {
        return buffer.limit() / entrySize;
    }

    public boolean isFull() {
        return entries >= capacity();
    }

    public boolean isEmpty() {
        return entries == 0;
    }

    public File file() {
        return file;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public int sizeInBytes() {
        return entries * entrySize;
    }

    protected void truncateToEntries(int newEntries) {
        for (int slot = newEntries; slot < entries; slot++) {
            for (int i = 0; i < entrySize; i++) {
                buffer.put(slotPosition(slot) + i, (byte) 0);
            }
        }
        entries = newEntries;
    }

    public void truncate() {
        truncateToEntries(0);
    }

    public void flush() {
        segment.force();
    }

    public void trimToValidSize() throws IOException {
        int validSize = sizeInBytes();
        unmap();
        channel.truncate(validSize);
        map(Math.max(validSize, entrySize));
    }

    public void delete() throws IOException {
        close();
        Files.deleteIfExists(file.toPath());
    }

    @Override
    public void close() throws IOException {
        unmap();
        channel.close();
    }
}
