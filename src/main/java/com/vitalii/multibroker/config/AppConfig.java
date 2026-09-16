package com.vitalii.multibroker.config;

import com.vitalii.multibroker.broker.activemq.ActiveMqConfig;
import com.vitalii.multibroker.broker.kafka.KafkaConfig;
import com.vitalii.multibroker.broker.rabbitmq.RabbitMqConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Properties;

public record AppConfig(
        String brokerType,
        String queueName,
        int consumersCount,
        long messagesCount,
        Path validCsvPath,
        Path invalidCsvPath,
        RabbitMqConfig rabbitMqConfig,
        ActiveMqConfig activeMqConfig,
        KafkaConfig kafkaConfig,
        int consumersCompletionTimeoutSeconds
) {
    public AppConfig {
        if (consumersCount < 1) {
            throw new IllegalArgumentException("consumers.count must be greater than zero");
        }
        if (messagesCount < 1) {
            throw new IllegalArgumentException("messages.count must be greater than zero");
        }
        if (consumersCompletionTimeoutSeconds < 1) {
            throw new IllegalArgumentException("consumers.completion.timeout.seconds must be greater than zero");
        }
    }

    public static AppConfig load() {
        try (InputStream inputStream = AppConfig.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {

            if (inputStream == null) {
                throw new IllegalStateException("application.properties not found");
            }

            Properties properties = new Properties();
            properties.load(inputStream);

            return fromProperties(properties);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load application.properties", e);
        }
    }

    static AppConfig fromProperties(Properties properties) {
        return new AppConfig(
                properties.getProperty("broker.type"),
                properties.getProperty("queue.name"),
                Integer.parseInt(properties.getProperty("consumers.count")),
                Long.parseLong(properties.getProperty("messages.count")),
                Path.of(properties.getProperty("csv.valid.path")),
                Path.of(properties.getProperty("csv.invalid.path")),
                new RabbitMqConfig(
                        properties.getProperty("rabbitmq.host"),
                        Integer.parseInt(properties.getProperty("rabbitmq.port")),
                        properties.getProperty("rabbitmq.username"),
                        properties.getProperty("rabbitmq.passwordd")),
                new ActiveMqConfig(
                        properties.getProperty("activemq.host"),
                        Integer.parseInt(properties.getProperty("activemq.port")),
                        properties.getProperty("activemq.username"),
                        properties.getProperty("activemq.passwordd")),
                new KafkaConfig(
                        properties.getProperty("kafka.bootstrap.servers"),
                        properties.getProperty("kafka.group.id"),
                        Integer.parseInt(properties.getProperty("kafka.partitions.count"))),
                Integer.parseInt(properties.getProperty("consumers.completion.timeout.seconds"))
        );
    }
}