package com.vitalii.multibroker.csv;

import com.vitalii.multibroker.model.PojoMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ValidCsvWriterTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void shouldWriteHeaderAndValidMessage() throws IOException {
        Path filePath = temporaryDirectory.resolve("valid.csv");

        PojoMessage message = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.now()
        );

        try (ValidCsvWriter writer = new ValidCsvWriter(filePath)) {
            writer.write(message);
        }

        List<String> lines = Files.readAllLines(filePath);

        assertEquals(
                List.of(
                        "name,count",
                        "anastasia,10"
                ),
                lines
        );
    }
}