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

public final class InvalidCsvWriter implements AutoCloseable {
    private static final String HEADER = "name,count,errors";

    private final BufferedWriter writer;

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
        try {
            String csvErrors = errorsToCsvJson(violations);
            writer.write(message.name() + "," + message.count() + "," + csvErrors);
            writer.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write invalid message", e);
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
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close invalid CSV file", e);
        }
    }
}
