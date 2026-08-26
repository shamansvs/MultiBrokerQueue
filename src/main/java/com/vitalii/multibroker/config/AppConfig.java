package com.vitalii.multibroker.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public record AppConfig(
        String brokerType,
        String brokerUrl,
        String queueName,
        int consumersCount,
        long messagesCount
) {
    public static AppConfig load() {
        Properties properties = new Properties();

        try (InputStream inputStream = AppConfig.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {

            if (inputStream == null) {
                throw new IllegalStateException("application.properties not found");
            }

            properties.load(inputStream);

            return new AppConfig(
                    properties.getProperty("broker.type"),
                    properties.getProperty("broker.url"),
                    properties.getProperty("queue.name"),
                    Integer.parseInt(properties.getProperty("consumers.count")),
                    Long.parseLong(properties.getProperty("messages.count"))
            );
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load application.properties", e);
        }
    }
}