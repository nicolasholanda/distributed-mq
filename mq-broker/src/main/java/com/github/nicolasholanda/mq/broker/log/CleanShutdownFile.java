package com.github.nicolasholanda.mq.broker.log;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public final class CleanShutdownFile {

    public static final String FILE_NAME = ".kafka_cleanshutdown";

    private final File file;

    public CleanShutdownFile(File dir) {
        this.file = new File(dir, FILE_NAME);
    }

    public boolean exists() {
        return file.exists();
    }

    public void create() throws IOException {
        Files.createDirectories(file.getParentFile().toPath());
        Files.write(file.toPath(), new byte[0]);
    }

    public void remove() throws IOException {
        Files.deleteIfExists(file.toPath());
    }

    public File file() {
        return file;
    }
}
