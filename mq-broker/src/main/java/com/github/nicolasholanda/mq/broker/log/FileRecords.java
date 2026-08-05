package com.github.nicolasholanda.mq.broker.log;

import com.github.nicolasholanda.mq.common.record.CorruptRecordException;
import com.github.nicolasholanda.mq.common.record.RecordBatch;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class FileRecords implements Closeable {

    private final File file;
    private final FileChannel channel;
    private int size;

    private FileRecords(File file, FileChannel channel, int size) {
        this.file = file;
        this.channel = channel;
        this.size = size;
    }

    public static FileRecords open(File file) throws IOException {
        FileChannel channel = FileChannel.open(file.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        channel.position(channel.size());
        return new FileRecords(file, channel, (int) channel.size());
    }

    public File file() {
        return file;
    }

    public int sizeInBytes() {
        return size;
    }

    public int append(RecordBatch batch) throws IOException {
        ByteBuffer buffer = batch.toBuffer();
        int written = 0;
        while (buffer.hasRemaining()) {
            written += channel.write(buffer);
        }
        size += written;
        return written;
    }

    public ByteBuffer read(int position, int length) throws IOException {
        int available = Math.max(0, Math.min(length, size - position));
        ByteBuffer buffer = ByteBuffer.allocate(available);
        int read = 0;
        while (buffer.hasRemaining()) {
            int bytes = channel.read(buffer, position + read);
            if (bytes < 0) {
                break;
            }
            read += bytes;
        }
        buffer.flip();
        return buffer;
    }

    public List<BatchPosition> batchesFrom(int startPosition) throws IOException {
        List<BatchPosition> batches = new ArrayList<>();
        Iterator<BatchPosition> iterator = batchIterator(startPosition);
        while (iterator.hasNext()) {
            batches.add(iterator.next());
        }
        return batches;
    }

    public Iterator<BatchPosition> batchIterator(int startPosition) throws IOException {
        ByteBuffer buffer = read(startPosition, size - startPosition);
        return new Iterator<>() {
            private int position = startPosition;

            @Override
            public boolean hasNext() {
                return buffer.remaining() >= RecordBatch.HEADER_SIZE;
            }

            @Override
            public BatchPosition next() {
                int current = position;
                int before = buffer.position();
                RecordBatch batch = RecordBatch.readFrom(buffer);
                int consumed = buffer.position() - before;
                position += consumed;
                return new BatchPosition(batch, current, consumed);
            }
        };
    }

    public void truncateTo(int newSize) throws IOException {
        if (newSize > size) {
            throw new IllegalArgumentException("Cannot truncate to a larger size: " + newSize + " > " + size);
        }
        channel.truncate(newSize);
        channel.position(newSize);
        size = newSize;
    }

    public void flush() throws IOException {
        channel.force(true);
    }

    public int validBytes() {
        int valid = 0;
        try {
            Iterator<BatchPosition> iterator = batchIterator(0);
            while (iterator.hasNext()) {
                BatchPosition batchPosition = iterator.next();
                valid = batchPosition.position() + batchPosition.sizeInBytes();
            }
        } catch (CorruptRecordException e) {
            return valid;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return valid;
    }

    @Override
    public void close() throws IOException {
        channel.force(true);
        channel.close();
    }

    public record BatchPosition(RecordBatch batch, int position, int sizeInBytes) {
    }
}
