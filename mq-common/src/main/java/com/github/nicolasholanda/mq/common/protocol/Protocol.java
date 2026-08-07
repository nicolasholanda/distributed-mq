package com.github.nicolasholanda.mq.common.protocol;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class Protocol {

    public static final int MAX_REQUEST_SIZE = 100 * 1024 * 1024;
    public static final short NULL_LENGTH = -1;

    private Protocol() {
    }

    public static void writeString(ByteBuf buf, String value) {
        if (value == null) {
            buf.writeShort(NULL_LENGTH);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > Short.MAX_VALUE) {
            throw new ProtocolException("String is too long to encode: " + bytes.length + " bytes");
        }
        buf.writeShort(bytes.length);
        buf.writeBytes(bytes);
    }

    public static String readString(ByteBuf buf) {
        short length = buf.readShort();
        if (length == NULL_LENGTH) {
            return null;
        }
        if (length < 0) {
            throw new ProtocolException("Invalid string length: " + length);
        }
        ensureReadable(buf, length);
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void writeBytes(ByteBuf buf, byte[] value) {
        if (value == null) {
            buf.writeInt(-1);
            return;
        }
        buf.writeInt(value.length);
        buf.writeBytes(value);
    }

    public static void writeBytes(ByteBuf buf, ByteBuffer value) {
        if (value == null) {
            buf.writeInt(-1);
            return;
        }
        buf.writeInt(value.remaining());
        buf.writeBytes(value.duplicate());
    }

    public static byte[] readBytes(ByteBuf buf) {
        int length = buf.readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0) {
            throw new ProtocolException("Invalid bytes length: " + length);
        }
        ensureReadable(buf, length);
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return bytes;
    }

    public static <T> void writeArray(ByteBuf buf, List<T> items, ArrayWriter<T> writer) {
        if (items == null) {
            buf.writeInt(-1);
            return;
        }
        buf.writeInt(items.size());
        for (T item : items) {
            writer.write(buf, item);
        }
    }

    public static <T> List<T> readArray(ByteBuf buf, Function<ByteBuf, T> reader) {
        int count = buf.readInt();
        if (count == -1) {
            return null;
        }
        if (count < 0) {
            throw new ProtocolException("Invalid array length: " + count);
        }
        List<T> items = new ArrayList<>(Math.min(count, 1024));
        for (int i = 0; i < count; i++) {
            items.add(reader.apply(buf));
        }
        return items;
    }

    public static void writeBoolean(ByteBuf buf, boolean value) {
        buf.writeByte(value ? 1 : 0);
    }

    public static boolean readBoolean(ByteBuf buf) {
        return buf.readByte() != 0;
    }

    private static void ensureReadable(ByteBuf buf, int length) {
        if (buf.readableBytes() < length) {
            throw new ProtocolException("Buffer underflow: needed " + length
                    + " bytes but only " + buf.readableBytes() + " readable");
        }
    }

    @FunctionalInterface
    public interface ArrayWriter<T> {
        void write(ByteBuf buf, T item);
    }
}
