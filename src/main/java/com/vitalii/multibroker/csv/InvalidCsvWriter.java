package com.vitalii.multibroker.csv;

import com.vitalii.multibroker.model.PojoMessage;
import jakarta.validation.ConstraintViolation;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InvalidCsvWriter implements AutoCloseable {
    private static final String HEADER = "name,count,errors";

    private static final Logger LOGGER = LoggerFactory.getLogger(InvalidCsvWriter.class);
    private final BufferedWriter writer;
    private long writtenMessagesCount;
    private long writingDurationNanos;

    public InvalidCsvWriter(Path filePath) {
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
            throw new UncheckedIOException("Failed to create invalid CSV file", e);
        }
    }

    public synchronized void write(PojoMessage message, Set<ConstraintViolation<PojoMessage>> violations) {
        String csvErrors = errorsToCsvJson(violations);
        long writeStart = System.nanoTime();

        try {
            writer.write(message.name() + "," + message.count() + "," + csvErrors);
            writer.newLine();
            writtenMessagesCount++;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write invalid message", e);
        } finally {
            writingDurationNanos += System.nanoTime() - writeStart;
        }
    }

    private String errorsToCsvJson(Set<ConstraintViolation<PojoMessage>> violations) {
        String errorsJson = violations.stream()
                .map(ConstraintViolation::getMessage)
                .map(error -> "\"" + error + "\"")
                .collect(Collectors.joining(
                        ",",
                        "{\"errors\":[",
                        "]}"
                ));
        return "\"" + errorsJson.replace("\"", "\"\"") + "\"";
    }

    @Override
    public synchronized void close() {
        try {
            writer.close();
            long messagesPerSecond = Math.round(writtenMessagesCount * 1_000_000_000.0
                    / Math.max(1, writingDurationNanos));
            LOGGER.info("Invalid CSV writer wrote {} messages ({} msg/s)",
                    writtenMessagesCount, messagesPerSecond);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close invalid CSV file", e);
        }
    }
}
