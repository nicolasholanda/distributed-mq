package com.github.nicolasholanda.mq.broker.log;

import com.github.nicolasholanda.mq.common.record.RecordBatch;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Log implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Log.class);

    private final File dir;
    private final LogConfig config;
    private final NavigableMap<Long, LogSegment> segments = new TreeMap<>();

    private long logStartOffset;
    private long logEndOffset;
    private long highWatermark;
    private long recoveryPoint;

    private Log(File dir, LogConfig config) {
        this.dir = dir;
        this.config = config;
    }

    public static Log open(File dir, LogConfig config) throws IOException {
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Could not create log directory " + dir);
        }
        Log instance = new Log(dir, config);
        instance.loadSegments();
        return instance;
    }

    private void loadSegments() throws IOException {
        List<Long> baseOffsets = new ArrayList<>();
        File[] files = dir.listFiles((unused, name) -> name.endsWith(LogSegment.LOG_SUFFIX));
        if (files != null) {
            for (File file : files) {
                baseOffsets.add(LogSegment.offsetFromFileName(file.getName()));
            }
        }
        baseOffsets.sort(Long::compareTo);
        if (baseOffsets.isEmpty()) {
            baseOffsets.add(0L);
        }
        for (long baseOffset : baseOffsets) {
            segments.put(baseOffset, openSegment(baseOffset));
        }
        logStartOffset = segments.firstKey();
        logEndOffset = activeSegment().nextOffset();
        highWatermark = logEndOffset;
        recoveryPoint = logEndOffset;
    }

    private LogSegment openSegment(long baseOffset) throws IOException {
        return LogSegment.open(dir, baseOffset, config.indexIntervalBytes(), config.maxIndexSize());
    }

    public LogAppendInfo append(RecordBatch batch, boolean assignOffsets) throws IOException {
        RecordBatch toAppend = assignOffsets ? batch.withBaseOffset(logEndOffset) : batch;
        if (toAppend.sizeInBytes() > config.maxMessageBytes()) {
            throw new RecordTooLargeException("Batch of " + toAppend.sizeInBytes()
                    + " bytes exceeds max.message.bytes " + config.maxMessageBytes());
        }
        if (!assignOffsets && toAppend.baseOffset() != logEndOffset) {
            throw new IllegalArgumentException("Follower append must be contiguous: expected base offset "
                    + logEndOffset + " but got " + toAppend.baseOffset());
        }
        maybeRoll(toAppend.sizeInBytes());
        LogAppendInfo info = activeSegment().append(toAppend);
        logEndOffset = toAppend.lastOffset() + 1;
        return info;
    }

    private void maybeRoll(int batchSize) throws IOException {
        LogSegment active = activeSegment();
        boolean sizeExceeded = !active.isEmpty() && active.sizeInBytes() + batchSize > config.segmentBytes();
        boolean timeExceeded = !active.isEmpty()
                && System.currentTimeMillis() - active.createdAtMs() >= config.segmentMs();
        if (sizeExceeded || timeExceeded) {
            roll();
        }
    }

    public void roll() throws IOException {
        LogSegment active = activeSegment();
        if (active.isEmpty()) {
            return;
        }
        active.flush();
        active.trim();
        LogSegment rolled = openSegment(logEndOffset);
        segments.put(logEndOffset, rolled);
        log.debug("Rolled new segment at offset {} in {}", logEndOffset, dir.getName());
    }

    public FetchDataInfo read(long startOffset, int maxBytes, IsolationLevel level) throws IOException {
        long maxReadableOffset = level == IsolationLevel.READ_COMMITTED ? highWatermark : logEndOffset;
        if (startOffset < logStartOffset || startOffset > logEndOffset) {
            throw new OffsetOutOfRangeException("Offset " + startOffset + " is out of range ["
                    + logStartOffset + ", " + logEndOffset + "]");
        }
        List<RecordBatch> collected = new ArrayList<>();
        int consumed = 0;
        long nextOffset = startOffset;
        Map.Entry<Long, LogSegment> entry = segments.floorEntry(startOffset);
        while (entry != null && consumed < maxBytes) {
            LogSegment segment = entry.getValue();
            for (RecordBatch batch : segment.readFrom(nextOffset, maxBytes - consumed)) {
                if (batch.lastOffset() >= maxReadableOffset) {
                    return new FetchDataInfo(startOffset, highWatermark, logStartOffset, collected);
                }
                collected.add(batch);
                consumed += batch.sizeInBytes();
                nextOffset = batch.lastOffset() + 1;
                if (consumed >= maxBytes) {
                    break;
                }
            }
            if (nextOffset < segment.nextOffset()) {
                break;
            }
            entry = segments.higherEntry(entry.getKey());
        }
        return new FetchDataInfo(startOffset, highWatermark, logStartOffset, collected);
    }

    public OffsetAndTimestamp offsetForTimestamp(long timestampMs) throws IOException {
        for (LogSegment segment : segments.values()) {
            OffsetAndTimestamp found = segment.offsetForTimestamp(timestampMs);
            if (found != OffsetAndTimestamp.NONE) {
                return found;
            }
        }
        return OffsetAndTimestamp.NONE;
    }

    public void truncateTo(long offset) throws IOException {
        if (offset >= logEndOffset) {
            return;
        }
        List<Long> toDelete = new ArrayList<>(segments.tailMap(offset, false).keySet());
        for (long baseOffset : toDelete) {
            segments.remove(baseOffset).delete();
        }
        LogSegment last = activeSegment();
        last.truncateTo(offset);
        logEndOffset = Math.max(last.nextOffset(), last.baseOffset());
        highWatermark = Math.min(highWatermark, logEndOffset);
        recoveryPoint = Math.min(recoveryPoint, logEndOffset);
    }

    public void updateHighWatermark(long newHighWatermark) {
        highWatermark = Math.max(logStartOffset, Math.min(newHighWatermark, logEndOffset));
    }

    public void flush() throws IOException {
        for (LogSegment segment : segments.values()) {
            segment.flush();
        }
        recoveryPoint = logEndOffset;
    }

    public LogSegment activeSegment() {
        return segments.lastEntry().getValue();
    }

    public NavigableMap<Long, LogSegment> segments() {
        return segments;
    }

    public int numberOfSegments() {
        return segments.size();
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public long logEndOffset() {
        return logEndOffset;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public long recoveryPoint() {
        return recoveryPoint;
    }

    public File dir() {
        return dir;
    }

    public LogConfig config() {
        return config;
    }

    public int sizeInBytes() {
        int size = 0;
        for (LogSegment segment : segments.values()) {
            size += segment.sizeInBytes();
        }
        return size;
    }

    @Override
    public void close() throws IOException {
        for (LogSegment segment : segments.values()) {
            segment.close();
        }
        segments.clear();
    }
}
