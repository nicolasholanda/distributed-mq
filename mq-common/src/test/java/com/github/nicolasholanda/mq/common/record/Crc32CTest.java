package com.github.nicolasholanda.mq.common.record;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;
import org.junit.jupiter.api.Test;

class Crc32CTest {

    @Test
    void matchesKnownVectors() {
        assertThat(Crc32C.compute(new byte[0], 0, 0)).isEqualTo(0L);
        assertThat(Crc32C.compute("123456789".getBytes(StandardCharsets.UTF_8), 0, 9)).isEqualTo(0xE3069283L);
        assertThat(Crc32C.compute(new byte[32], 0, 32)).isEqualTo(0x8A9136AAL);
    }

    @Test
    void matchesJdkImplementation() {
        byte[] data = "the quick brown fox".getBytes(StandardCharsets.UTF_8);
        Checksum jdk = new CRC32C();
        jdk.update(data, 0, data.length);

        assertThat(Crc32C.compute(data, 0, data.length)).isEqualTo(jdk.getValue());
    }

    @Test
    void incrementalChecksumMatchesOneShot() {
        byte[] data = "incremental update path".getBytes(StandardCharsets.UTF_8);
        Checksum checksum = Crc32C.create();
        checksum.update(data, 0, 10);
        checksum.update(data, 10, data.length - 10);

        assertThat(checksum.getValue()).isEqualTo(Crc32C.compute(data, 0, data.length));
    }

    @Test
    void respectsOffsetAndLength() {
        byte[] data = "xxhello".getBytes(StandardCharsets.UTF_8);

        assertThat(Crc32C.compute(data, 2, 5))
                .isEqualTo(Crc32C.compute("hello".getBytes(StandardCharsets.UTF_8), 0, 5));
    }
}
