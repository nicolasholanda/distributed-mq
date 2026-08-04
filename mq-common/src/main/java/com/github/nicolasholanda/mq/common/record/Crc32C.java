package com.github.nicolasholanda.mq.common.record;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

public final class Crc32C {

    private static final int[] TABLE = new int[256];
    private static final int POLYNOMIAL = 0x82F63B78;

    static {
        for (int i = 0; i < 256; i++) {
            int crc = i;
            for (int bit = 0; bit < 8; bit++) {
                crc = (crc & 1) != 0 ? (crc >>> 1) ^ POLYNOMIAL : crc >>> 1;
            }
            TABLE[i] = crc;
        }
    }

    private Crc32C() {
    }

    public static long compute(ByteBuffer buffer, int offset, int length) {
        int crc = 0xFFFFFFFF;
        for (int i = offset; i < offset + length; i++) {
            crc = (crc >>> 8) ^ TABLE[(crc ^ buffer.get(i)) & 0xFF];
        }
        return (~crc) & 0xFFFFFFFFL;
    }

    public static long compute(byte[] bytes, int offset, int length) {
        return compute(ByteBuffer.wrap(bytes), offset, length);
    }

    public static Checksum create() {
        return new Checksum() {
            private int crc = 0xFFFFFFFF;

            @Override
            public void update(int b) {
                crc = (crc >>> 8) ^ TABLE[(crc ^ b) & 0xFF];
            }

            @Override
            public void update(byte[] b, int off, int len) {
                for (int i = off; i < off + len; i++) {
                    update(b[i]);
                }
            }

            @Override
            public long getValue() {
                return (~crc) & 0xFFFFFFFFL;
            }

            @Override
            public void reset() {
                crc = 0xFFFFFFFF;
            }
        };
    }
}
