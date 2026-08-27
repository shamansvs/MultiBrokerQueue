package com.vitalii.multibroker.csv;

import com.vitalii.multibroker.model.PojoMessage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class ValidCsvWriter implements AutoCloseable {
    private static final String HEADER = "name,count";

    private final BufferedWriter writer;

    public ValidCsvWriter(Path filePath) {
        try {
            writer = Files.newBufferedWriter(
                    filePath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);

            writer.write(HEADER);
            writer.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create valid CSV file", e);
        }
    }

    public synchronized void write(PojoMessage message) {
        try {
            writer.write(message.name() + "," + message.count());
            writer.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write valid message", e);
        }
    }

    @Override
    public synchronized void close() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close valid CSV file", e);
        }
    }
}