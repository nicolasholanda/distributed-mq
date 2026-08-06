package com.github.nicolasholanda.mq.broker.log;

import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogRecovery {

    private static final Logger log = LoggerFactory.getLogger(LogRecovery.class);

    private LogRecovery() {
    }

    public static RecoveryResult recover(List<LogSegment> segments, long recoveryPoint, boolean cleanShutdown)
            throws IOException {
        int scanned = 0;
        int truncatedSegments = 0;
        long endOffset = segments.getFirst().baseOffset();
        for (LogSegment segment : segments) {
            boolean needsScan = !cleanShutdown || segment.needsRecovery() || segment.nextOffset() > recoveryPoint;
            if (needsScan) {
                int sizeBefore = segment.sizeInBytes();
                segment.recoverAndRebuildIndexes();
                scanned++;
                if (segment.sizeInBytes() < sizeBefore) {
                    truncatedSegments++;
                    log.warn("Truncated corrupted tail of segment {} from {} to {} bytes",
                            segment.file().getName(), sizeBefore, segment.sizeInBytes());
                }
            }
            endOffset = Math.max(endOffset, segment.nextOffset());
        }
        return new RecoveryResult(endOffset, scanned, truncatedSegments);
    }

    public record RecoveryResult(long logEndOffset, int segmentsScanned, int segmentsTruncated) {
    }
}
