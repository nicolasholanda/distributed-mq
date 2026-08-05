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

    private final long baseOffset;
    private final FileRecords records;
    private final long createdAtMs;

    private long nextOffset;
    private long maxTimestamp = -1;
    private long offsetOfMaxTimestamp = -1;

    private LogSegment(long baseOffset, FileRecords records, long createdAtMs) {
        this.baseOffset = baseOffset;
        this.records = records;
        this.createdAtMs = createdAtMs;
        this.nextOffset = baseOffset;
    }

    public static LogSegment open(File dir, long baseOffset) throws IOException {
        File logFile = new File(dir, fileName(baseOffset, LOG_SUFFIX));
        LogSegment segment = new LogSegment(baseOffset, FileRecords.open(logFile), System.currentTimeMillis());
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
        for (FileRecords.BatchPosition batchPosition : records.batchesFrom(0)) {
            observe(batchPosition.batch());
        }
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
        int written = records.append(batch);
        observe(batch);
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
        Iterator<FileRecords.BatchPosition> iterator = records.batchIterator(0);
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
        nextOffset = newNextOffset;
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

    public void flush() throws IOException {
        records.flush();
    }

    public void delete() throws IOException {
        records.close();
        Files.deleteIfExists(records.file().toPath());
    }

    @Override
    public void close() throws IOException {
        records.close();
    }
}
