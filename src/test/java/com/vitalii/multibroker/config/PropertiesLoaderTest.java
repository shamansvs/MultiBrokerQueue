package com.vitalii.multibroker.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PropertiesLoaderTest {

    @TempDir
    Path tempDirectory;

    @Test
    void shouldLoadPropertiesFromFileUsingUtf8() throws IOException {
        Path configPath = tempDirectory.resolve("application.properties");

        Files.writeString(configPath,
                """
                        queue.name=test-messages
                        description=Тестова конфігурація
                        """,
                StandardCharsets.UTF_8);
        Properties properties = PropertiesLoader.loadFromFile(configPath);
        assertAll(() -> assertEquals("test-messages", properties.getProperty("queue.name")),
                () -> assertEquals("Тестова конфігурація", properties.getProperty("description")));
    }

    @Test
    void shouldLoadPropertiesFromResourcesUsingUtf8() {
        Properties properties = PropertiesLoader.loadFromResources("config/test-application.properties");
        assertAll(() -> assertEquals("test-messages", properties.getProperty("queue.name")),
                () -> assertEquals("Тестова конфігурація", properties.getProperty("description")));
    }

    @Test
    void shouldFailWhenFileDoesNotExist() {
        Path configPath = tempDirectory.resolve("missing.properties");
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> PropertiesLoader.loadFromFile(configPath));

        assertEquals("Cannot load properties from file: " + configPath.toAbsolutePath(),
                exception.getMessage());
        assertInstanceOf(IOException.class, exception.getCause());
    }

    @Test
    void shouldFailWhenResourceDoesNotExist() {
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> PropertiesLoader.loadFromResources("config/missing.properties"));
        assertEquals("Properties resource not found: config/missing.properties", exception.getMessage());
    }
}