package com.vitalii.multibroker.csv;

import com.vitalii.multibroker.model.PojoMessage;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InvalidCsvWriterTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void shouldWriteInvalidMessageWithErrorsAsJson() throws IOException {
        Path filePath = temporaryDirectory.resolve("invalid.csv");

        PojoMessage message = new PojoMessage(
                "anastasia",
                "",
                10,
                LocalDateTime.now()
        );

        try (ValidatorFactory factory = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory();
             InvalidCsvWriter writer = new InvalidCsvWriter(filePath)) {

            Set<ConstraintViolation<PojoMessage>> violations =
                    factory.getValidator().validate(message);

            writer.write(message, violations);
        }

        List<String> lines = Files.readAllLines(filePath);

        assertEquals(
                List.of(
                        "name,count,errors",
                        "anastasia,10,\"{\"\"errors\"\":[\"\"eddr must not be blank\"\"]}\""
                ),
                lines
        );
    }
}