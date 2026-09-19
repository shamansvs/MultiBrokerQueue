package com.vitalii.multibroker.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public final class PropertiesLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesLoader.class);

    private PropertiesLoader() {
    }

    public static Properties loadFromFile(Path path) {
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            Properties properties = new Properties();
            properties.load(reader);
            LOGGER.info("Loaded properties from file: {}", path.toAbsolutePath());
            return properties;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load properties from file: " + path.toAbsolutePath(), e);
        }
    }

    public static Properties loadFromResources(String resourceName) {
        try (InputStream inputStream = PropertiesLoader.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new IllegalStateException("Properties resource not found: " + resourceName);
            }
            try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                Properties properties = new Properties();
                properties.load(reader);
                LOGGER.info("Loaded properties from resource: {}", resourceName);
                return properties;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load properties from resource: " + resourceName, e);
        }
    }
}