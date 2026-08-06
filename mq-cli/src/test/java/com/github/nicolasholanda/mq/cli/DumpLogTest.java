package com.github.nicolasholanda.mq.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.nicolasholanda.mq.broker.log.Log;
import com.github.nicolasholanda.mq.broker.log.LogConfig;
import com.github.nicolasholanda.mq.common.record.RecordBatchBuilder;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DumpLogTest {

    private static final LogConfig CONFIG = LogConfig.defaults()
            .withSegmentBytes(4096)
            .withIndexIntervalBytes(256)
            .withMaxIndexSize(8 * 1024);

    @TempDir
    File dir;

    @Test
    void printsBatchMetadata() throws IOException {
        writeLog(5);

        String output = dump(new DumpLog.Options(List.of(firstSegment()), false, false));

        assertThat(output).contains("Starting offset: 0");
        assertThat(output).contains("baseOffset: 0 lastOffset: 0 count: 1");
        assertThat(output).contains("baseOffset: 4 lastOffset: 4 count: 1");
        assertThat(output).doesNotContain("| offset:");
    }

    @Test
    void printsRecordPayloadsWithPrintDataLog() throws IOException {
        writeLog(3);

        String output = dump(new DumpLog.Options(List.of(firstSegment()), true, false));

        assertThat(output).contains("| offset: 0").contains("value: v0");
        assertThat(output).contains("| offset: 2").contains("value: v2");
        assertThat(output).contains("key: null");
    }

    @Test
    void reportsIndexSanity() throws IOException {
        writeLog(200);

        String output = dump(new DumpLog.Options(List.of(firstSegment()), false, true));

        assertThat(output).contains("Index entries:");
    }

    @Test
    void reportsCorruptionAndExitsNonZero() throws IOException {
        writeLog(10);
        File segment = firstSegment();
        try (RandomAccessFile raf = new RandomAccessFile(segment, "rw")) {
            raf.seek(raf.length() - 2);
            raf.write(raf.read() ^ 0xFF);
        }

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int exitCode = new DumpLog(new PrintStream(buffer, true, StandardCharsets.UTF_8))
                .run(new DumpLog.Options(List.of(segment), false, false));

        assertThat(exitCode).isEqualTo(1);
        assertThat(buffer.toString(StandardCharsets.UTF_8)).contains("CORRUPT:");
    }

    @Test
    void reportsMissingFile() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int exitCode = new DumpLog(new PrintStream(buffer, true, StandardCharsets.UTF_8))
                .run(new DumpLog.Options(List.of(new File(dir, "missing.log")), false, false));

        assertThat(exitCode).isEqualTo(1);
        assertThat(buffer.toString(StandardCharsets.UTF_8)).contains("File not found");
    }

    @Test
    void parsesArguments() {
        DumpLog.Options options = DumpLog.Options.parse(
                new String[] {"--files", "a.log,b.log", "--print-data-log", "--index-sanity-check"});

        assertThat(options.files()).extracting(File::getName).containsExactly("a.log", "b.log");
        assertThat(options.printDataLog()).isTrue();
        assertThat(options.indexSanityCheck()).isTrue();
        assertThat(DumpLog.Options.parse(new String[] {"--print-data-log"})).isNull();
        assertThat(DumpLog.Options.parse(new String[] {"--unknown"})).isNull();
    }

    private void writeLog(int records) throws IOException {
        try (Log log = Log.open(dir, CONFIG)) {
            for (int i = 0; i < records; i++) {
                log.append(new RecordBatchBuilder()
                        .append(1_000L + i, null, ("v" + i).getBytes(StandardCharsets.UTF_8))
                        .build(), true);
            }
        }
    }

    private File firstSegment() {
        return new File(dir, "00000000000000000000.log");
    }

    private String dump(DumpLog.Options options) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        new DumpLog(new PrintStream(buffer, true, StandardCharsets.UTF_8)).run(options);
        return buffer.toString(StandardCharsets.UTF_8);
    }
}
