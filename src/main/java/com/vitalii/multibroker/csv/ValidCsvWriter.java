package com.vitalii.multibroker.csv;

import com.vitalii.multibroker.model.PojoMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(ValidCsvWriter.class);
    private long writtenMessagesCount;
    private long writingDurationNanos;

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
        long writeStart = System.nanoTime();
        try {
            writer.write(message.name() + "," + message.count());
            writer.newLine();
            writtenMessagesCount++;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write valid message", e);
        } finally {
            writingDurationNanos += System.nanoTime() - writeStart;
        }
    }

    @Override
    public synchronized void close() {
        try {
            writer.close();
            long messagesPerSecond = Math.round(writtenMessagesCount * 1_000_000_000.0
                    / Math.max(1, writingDurationNanos));

            LOGGER.info(
                    "Valid CSV writer wrote {} messages ({} msg/s)", writtenMessagesCount, messagesPerSecond);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close valid CSV file", e);
        }
    }
}