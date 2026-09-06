package com.vitalii.multibroker.config;

import com.vitalii.multibroker.broker.rabbitmq.RabbitMqConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Properties;

public record AppConfig(
        String brokerType,
        String queueName,
        int producersCount,
        int consumersCount,
        long messagesCount,
        Path validCsvPath,
        Path invalidCsvPath,
        RabbitMqConfig rabbitMqConfig
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
                    properties.getProperty("queue.name"),
                    Integer.parseInt(properties.getProperty("producers.count")),
                    Integer.parseInt(properties.getProperty("consumers.count")),
                    Long.parseLong(properties.getProperty("messages.count")),
                    Path.of(properties.getProperty("csv.valid.path")),
                    Path.of(properties.getProperty("csv.invalid.path")),
                    new RabbitMqConfig(
                            properties.getProperty("rabbitmq.host"),
                            Integer.parseInt(properties.getProperty("rabbitmq.port")),
                            properties.getProperty("rabbitmq.username"),
                            properties.getProperty("rabbitmq.password")
                    )
            );
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load application.properties", e);
        }
    }
}