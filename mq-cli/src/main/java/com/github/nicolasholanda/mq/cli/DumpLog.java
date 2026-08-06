package com.github.nicolasholanda.mq.cli;

import com.github.nicolasholanda.mq.broker.log.FileRecords;
import com.github.nicolasholanda.mq.broker.log.LogSegment;
import com.github.nicolasholanda.mq.broker.log.OffsetIndex;
import com.github.nicolasholanda.mq.common.record.CorruptRecordException;
import com.github.nicolasholanda.mq.common.record.Record;
import com.github.nicolasholanda.mq.common.record.RecordBatch;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class DumpLog {

    private final PrintStream out;

    public DumpLog(PrintStream out) {
        this.out = out;
    }

    public static void main(String[] args) throws IOException {
        Options options = Options.parse(args);
        if (options == null) {
            printUsage(System.out);
            System.exit(1);
            return;
        }
        int exitCode = new DumpLog(System.out).run(options);
        System.exit(exitCode);
    }

    public static void printUsage(PrintStream out) {
        out.println("usage: mq-dump-log --files <file.log>[,<file.log>...] [--print-data-log] [--index-sanity-check]");
    }

    public int run(Options options) throws IOException {
        int corrupted = 0;
        for (File file : options.files()) {
            if (!file.exists()) {
                out.println("File not found: " + file);
                corrupted++;
                continue;
            }
            corrupted += dumpFile(file, options);
        }
        return corrupted == 0 ? 0 : 1;
    }

    private int dumpFile(File file, Options options) throws IOException {
        out.println("Dumping " + file.getPath());
        long baseOffset = LogSegment.offsetFromFileName(file.getName());
        out.println("Starting offset: " + baseOffset);
        int corrupted = 0;
        try (FileRecords records = FileRecords.open(file)) {
            Iterator<FileRecords.BatchPosition> iterator = records.batchIterator(0);
            while (true) {
                FileRecords.BatchPosition batchPosition;
                try {
                    if (!iterator.hasNext()) {
                        break;
                    }
                    batchPosition = iterator.next();
                } catch (CorruptRecordException e) {
                    out.println("CORRUPT: " + e.getMessage());
                    corrupted++;
                    break;
                }
                printBatch(batchPosition, options.printDataLog());
            }
        }
        if (options.indexSanityCheck()) {
            corrupted += checkIndex(file, baseOffset);
        }
        return corrupted;
    }

    private void printBatch(FileRecords.BatchPosition batchPosition, boolean printDataLog) {
        RecordBatch batch = batchPosition.batch();
        out.printf("baseOffset: %d lastOffset: %d count: %d position: %d size: %d "
                        + "magic: %d compression: %s isControl: %b "
                        + "baseTimestamp: %d maxTimestamp: %d leaderEpoch: %d "
                        + "producerId: %d producerEpoch: %d baseSequence: %d%n",
                batch.baseOffset(), batch.lastOffset(), batch.recordCount(),
                batchPosition.position(), batchPosition.sizeInBytes(),
                batch.magic(), batch.compressionType(), batch.isControlBatch(),
                batch.baseTimestamp(), batch.maxTimestamp(), batch.partitionLeaderEpoch(),
                batch.producerId(), batch.producerEpoch(), batch.baseSequence());
        if (!printDataLog) {
            return;
        }
        for (Record record : batch.records()) {
            out.printf("| offset: %d timestamp: %d keySize: %d valueSize: %d key: %s value: %s%n",
                    batch.baseOffset() + record.offsetDelta(),
                    batch.baseTimestamp() + record.timestampDelta(),
                    record.key() == null ? -1 : record.key().length,
                    record.value() == null ? -1 : record.value().length,
                    text(record.key()), text(record.value()));
        }
    }

    private int checkIndex(File logFile, long baseOffset) throws IOException {
        File indexFile = new File(logFile.getParentFile(),
                LogSegment.fileName(baseOffset, OffsetIndex.INDEX_SUFFIX));
        if (!indexFile.exists()) {
            out.println("Index file missing: " + indexFile.getName());
            return 1;
        }
        try (OffsetIndex index = new OffsetIndex(indexFile, baseOffset, (int) indexFile.length())) {
            out.println("Index entries: " + index.entries() + " lastOffset: " + index.lastOffset());
            long previous = -1;
            for (int slot = 0; slot < index.entries(); slot++) {
                long offset = index.entryAt(slot).offset();
                if (offset <= previous) {
                    out.println("Index is not monotonic at slot " + slot);
                    return 1;
                }
                previous = offset;
            }
        }
        return 0;
    }

    private static String text(byte[] bytes) {
        return bytes == null ? "null" : new String(bytes, StandardCharsets.UTF_8);
    }

    public record Options(List<File> files, boolean printDataLog, boolean indexSanityCheck) {

        public static Options parse(String[] args) {
            List<File> files = new ArrayList<>();
            boolean printDataLog = false;
            boolean indexSanityCheck = false;
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--files" -> {
                        if (i + 1 >= args.length) {
                            return null;
                        }
                        for (String path : args[++i].split(",")) {
                            files.add(new File(path.trim()));
                        }
                    }
                    case "--print-data-log" -> printDataLog = true;
                    case "--index-sanity-check" -> indexSanityCheck = true;
                    default -> {
                        return null;
                    }
                }
            }
            return files.isEmpty() ? null : new Options(files, printDataLog, indexSanityCheck);
        }
    }
}
