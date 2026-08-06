package com.github.nicolasholanda.mq.broker.log;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.nicolasholanda.mq.common.record.RecordBatch;
import com.github.nicolasholanda.mq.common.record.RecordBatchBuilder;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogRecoveryTest {

    private static final LogConfig CONFIG = LogConfig.defaults()
            .withSegmentBytes(4096)
            .withIndexIntervalBytes(256)
            .withMaxIndexSize(8 * 1024);

    @TempDir
    File dir;

    @Test
    void cleanShutdownLeavesMarkerAndRecoveryPoint() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 100; i++) {
                log.append(batch("v" + i), true);
            }
        }

        assertThat(new File(dir, CleanShutdownFile.FILE_NAME)).exists();
        assertThat(new File(dir, Log.RECOVERY_POINT_FILE)).hasContent("100");

        try (Log reopened = Log.open(dir, CONFIG)) {
            assertThat(reopened.logEndOffset()).isEqualTo(100L);
            assertThat(new File(dir, CleanShutdownFile.FILE_NAME)).doesNotExist();
        }
    }

    @Test
    void recoversToLastIntactBatchAfterTornWrite() throws IOException {
        long endOffsetBefore;
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 300; i++) {
                log.append(batch("v" + i), true);
            }
            endOffsetBefore = log.logEndOffset();
        }
        new File(dir, CleanShutdownFile.FILE_NAME).delete();

        File activeLog = lastLogFile();
        try (RandomAccessFile raf = new RandomAccessFile(activeLog, "rw")) {
            raf.setLength(raf.length() - 40);
        }

        try (Log recovered = Log.open(dir, CONFIG)) {
            assertThat(endOffsetBefore).isEqualTo(300L);
            assertThat(recovered.logEndOffset()).isLessThan(300L).isGreaterThan(280L);
            assertThat(readAll(recovered)).hasSize((int) recovered.logEndOffset());
        }
    }

    @Test
    void recoversWhenLastBatchPayloadIsCorrupted() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 200; i++) {
                log.append(batch("v" + i), true);
            }
        }
        new File(dir, CleanShutdownFile.FILE_NAME).delete();

        File activeLog = lastLogFile();
        try (RandomAccessFile raf = new RandomAccessFile(activeLog, "rw")) {
            raf.seek(raf.length() - 3);
            int corrupted = raf.read() ^ 0xFF;
            raf.seek(raf.length() - 3);
            raf.write(corrupted);
        }

        try (Log recovered = Log.open(dir, CONFIG)) {
            assertThat(recovered.logEndOffset()).isEqualTo(199L);
            assertThat(readAll(recovered)).hasSize(199);
        }
    }

    @Test
    void rebuildsIndexesDeletedWhileOffline() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 400; i++) {
                log.append(batch("v" + i), true);
            }
        }
        new File(dir, CleanShutdownFile.FILE_NAME).delete();

        List<String> deleted = new ArrayList<>();
        File[] files = dir.listFiles((unused, name) ->
                name.endsWith(OffsetIndex.INDEX_SUFFIX) || name.endsWith(TimeIndex.TIME_INDEX_SUFFIX));
        for (File file : files) {
            deleted.add(file.getName());
            assertThat(file.delete()).isTrue();
        }

        try (Log recovered = Log.open(dir, CONFIG)) {
            assertThat(deleted).isNotEmpty();
            assertThat(recovered.logEndOffset()).isEqualTo(400L);
            assertThat(readAll(recovered)).hasSize(400);
            for (LogSegment segment : recovered.segments().values()) {
                if (!segment.isEmpty()) {
                    assertThat(segment.offsetIndex().entries()).isPositive();
                }
            }
        }
    }

    @Test
    void readsAreIdenticalBeforeAndAfterIndexRebuild() throws IOException {
        List<String> before;
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 250; i++) {
                log.append(batch("v" + i), true);
            }
            before = readAll(log);
        }

        File[] indexes = dir.listFiles((unused, name) -> name.endsWith(OffsetIndex.INDEX_SUFFIX));
        for (File index : indexes) {
            assertThat(index.delete()).isTrue();
        }

        try (Log reopened = Log.open(dir, CONFIG)) {
            assertThat(readAll(reopened)).isEqualTo(before);
        }
    }

    @Test
    void appendsContinueAfterRecovery() throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < 120; i++) {
                log.append(batch("v" + i), true);
            }
        }
        new File(dir, CleanShutdownFile.FILE_NAME).delete();

        try (Log recovered = Log.open(dir, CONFIG)) {
            long resumeOffset = recovered.logEndOffset();
            recovered.append(batch("after-recovery"), true);

            assertThat(recovered.logEndOffset()).isEqualTo(resumeOffset + 1);
            assertThat(readAll(recovered).getLast()).isEqualTo("after-recovery");
        }
    }

    private File lastLogFile() {
        File[] files = dir.listFiles((unused, name) -> name.endsWith(LogSegment.LOG_SUFFIX));
        File last = files[0];
        for (File file : files) {
            if (file.getName().compareTo(last.getName()) > 0) {
                last = file;
            }
        }
        return last;
    }

    private static RecordBatch batch(String value) {
        return new RecordBatchBuilder()
                .append(System.currentTimeMillis(), null, value.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    private static List<String> readAll(Log log) throws IOException {
        List<String> values = new ArrayList<>();
        long offset = log.logStartOffset();
        while (offset < log.logEndOffset()) {
            FetchDataInfo fetch = log.read(offset, 8192, IsolationLevel.READ_UNCOMMITTED);
            if (fetch.isEmpty()) {
                break;
            }
            for (RecordBatch batch : fetch.batches()) {
                values.add(new String(batch.records().getFirst().value(), StandardCharsets.UTF_8));
            }
            offset = fetch.batches().getLast().lastOffset() + 1;
        }
        return values;
    }
}
