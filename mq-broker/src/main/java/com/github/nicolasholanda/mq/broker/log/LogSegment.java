package com.github.nicolasholanda.mq.broker.log;

import com.github.nicolasholanda.mq.common.record.RecordBatch;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class LogSegment implements Closeable {

    public static final String LOG_SUFFIX = ".log";
    public static final int OFFSET_FILE_NAME_LENGTH = 20;
    public static final int DEFAULT_INDEX_INTERVAL_BYTES = 4096;
    public static final int DEFAULT_MAX_INDEX_SIZE = 10 * 1024 * 1024;

    private final long baseOffset;
    private final FileRecords records;
    private final OffsetIndex offsetIndex;
    private final int indexIntervalBytes;
    private final long createdAtMs;

    private long nextOffset;
    private long maxTimestamp = -1;
    private long offsetOfMaxTimestamp = -1;
    private int bytesSinceLastIndexEntry;

    private LogSegment(long baseOffset, FileRecords records, OffsetIndex offsetIndex,
            int indexIntervalBytes, long createdAtMs) {
        this.baseOffset = baseOffset;
        this.records = records;
        this.offsetIndex = offsetIndex;
        this.indexIntervalBytes = indexIntervalBytes;
        this.createdAtMs = createdAtMs;
        this.nextOffset = baseOffset;
    }

    public static LogSegment open(File dir, long baseOffset) throws IOException {
        return open(dir, baseOffset, DEFAULT_INDEX_INTERVAL_BYTES, DEFAULT_MAX_INDEX_SIZE);
    }

    public static LogSegment open(File dir, long baseOffset, int indexIntervalBytes, int maxIndexSize)
            throws IOException {
        File logFile = new File(dir, fileName(baseOffset, LOG_SUFFIX));
        LogSegment segment = new LogSegment(baseOffset, FileRecords.open(logFile),
                OffsetIndex.open(dir, baseOffset, maxIndexSize), indexIntervalBytes, System.currentTimeMillis());
        segment.recoverState();
        return segment;
    }

    public static String fileName(long offset, String suffix) {
        return String.format("%0" + OFFSET_FILE_NAME_LENGTH + "d%s", offset, suffix);
    }

    public static long offsetFromFileName(String fileName) {
        return Long.parseLong(fileName.substring(0, OFFSET_FILE_NAME_LENGTH));
    }

    private void recoverState() throws IOException {
        int valid = records.validBytes();
        if (valid < records.sizeInBytes()) {
            records.truncateTo(valid);
        }
        rebuildIndex();
    }

    public void rebuildIndex() throws IOException {
        offsetIndex.truncate();
        bytesSinceLastIndexEntry = 0;
        nextOffset = baseOffset;
        maxTimestamp = -1;
        offsetOfMaxTimestamp = -1;
        for (FileRecords.BatchPosition batchPosition : records.batchesFrom(0)) {
            observe(batchPosition.batch());
            maybeIndex(batchPosition.batch(), batchPosition.position(), batchPosition.sizeInBytes());
        }
    }

    private void maybeIndex(RecordBatch batch, int position, int sizeInBytes) {
        if (bytesSinceLastIndexEntry >= indexIntervalBytes && !offsetIndex.isFull()
                && batch.baseOffset() > offsetIndex.lastOffset()) {
            offsetIndex.append(batch.baseOffset(), position);
            bytesSinceLastIndexEntry = 0;
        }
        bytesSinceLastIndexEntry += sizeInBytes;
    }

    private void observe(RecordBatch batch) {
        nextOffset = batch.lastOffset() + 1;
        if (batch.maxTimestamp() > maxTimestamp) {
            maxTimestamp = batch.maxTimestamp();
            offsetOfMaxTimestamp = batch.lastOffset();
        }
    }

    public LogAppendInfo append(RecordBatch batch) throws IOException {
        if (batch.baseOffset() < nextOffset) {
            throw new IllegalArgumentException("Batch base offset " + batch.baseOffset()
                    + " is behind the segment next offset " + nextOffset);
        }
        int position = records.sizeInBytes();
        int written = records.append(batch);
        observe(batch);
        maybeIndex(batch, position, written);
        return new LogAppendInfo(batch.baseOffset(), batch.lastOffset(), batch.maxTimestamp(),
                System.currentTimeMillis(), batch.recordCount(), written);
    }

    public List<RecordBatch> readFrom(long startOffset, int maxBytes) throws IOException {
        int position = positionOf(startOffset);
        if (position < 0) {
            return List.of();
        }
        List<RecordBatch> result = new ArrayList<>();
        int consumed = 0;
        Iterator<FileRecords.BatchPosition> iterator = records.batchIterator(position);
        while (iterator.hasNext()) {
            FileRecords.BatchPosition batchPosition = iterator.next();
            RecordBatch batch = batchPosition.batch();
            if (batch.lastOffset() < startOffset) {
                continue;
            }
            if (consumed > 0 && consumed + batchPosition.sizeInBytes() > maxBytes) {
                break;
            }
            result.add(batch);
            consumed += batchPosition.sizeInBytes();
            if (consumed >= maxBytes) {
                break;
            }
        }
        return result;
    }

    public ByteBuffer readRaw(int position, int length) throws IOException {
        return records.read(position, length);
    }

    public int positionOf(long targetOffset) throws IOException {
        IndexEntry entry = offsetIndex.lookup(targetOffset);
        int startPosition = entry.offset() < 0 ? 0 : entry.position();
        Iterator<FileRecords.BatchPosition> iterator = records.batchIterator(startPosition);
        while (iterator.hasNext()) {
            FileRecords.BatchPosition batchPosition = iterator.next();
            if (batchPosition.batch().lastOffset() >= targetOffset) {
                return batchPosition.position();
            }
        }
        return -1;
    }

    public void truncateTo(long offset) throws IOException {
        int position = 0;
        long newNextOffset = baseOffset;
        Iterator<FileRecords.BatchPosition> iterator = records.batchIterator(0);
        while (iterator.hasNext()) {
            FileRecords.BatchPosition batchPosition = iterator.next();
            if (batchPosition.batch().baseOffset() >= offset) {
                break;
            }
            position = batchPosition.position() + batchPosition.sizeInBytes();
            newNextOffset = batchPosition.batch().lastOffset() + 1;
        }
        records.truncateTo(position);
        offsetIndex.truncateTo(offset);
        nextOffset = newNextOffset;
        bytesSinceLastIndexEntry = 0;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long nextOffset() {
        return nextOffset;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public long offsetOfMaxTimestamp() {
        return offsetOfMaxTimestamp;
    }

    public long createdAtMs() {
        return createdAtMs;
    }

    public int sizeInBytes() {
        return records.sizeInBytes();
    }

    public boolean isEmpty() {
        return records.sizeInBytes() == 0;
    }

    public File file() {
        return records.file();
    }

    public OffsetIndex offsetIndex() {
        return offsetIndex;
    }

    public void flush() throws IOException {
        records.flush();
        offsetIndex.flush();
    }

    public void delete() throws IOException {
        records.close();
        offsetIndex.delete();
        Files.deleteIfExists(records.file().toPath());
    }

    public void trim() throws IOException {
        offsetIndex.trimToValidSize();
    }

    @Override
    public void close() throws IOException {
        records.close();
        offsetIndex.close();
    }
}
