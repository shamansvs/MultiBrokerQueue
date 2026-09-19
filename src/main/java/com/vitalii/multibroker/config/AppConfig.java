package com.vitalii.multibroker.config;

import com.vitalii.multibroker.broker.activemq.ActiveMqConfig;
import com.vitalii.multibroker.broker.inmemory.InMemoryConfig;
import com.vitalii.multibroker.broker.kafka.KafkaConfig;
import com.vitalii.multibroker.broker.rabbitmq.RabbitMqConfig;

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
        int consumersCompletionTimeoutSeconds,
        InMemoryConfig inMemoryConfig
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
        return fromProperties(PropertiesLoader.loadFromResources("application.properties"));
    }

    public static AppConfig load(Path configPath) {
        return fromProperties(PropertiesLoader.loadFromFile(configPath));
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
                Integer.parseInt(properties.getProperty("consumers.completion.timeout.seconds")),
                new InMemoryConfig(
                        Integer.parseInt(properties.getProperty("inmemory.queue.capacity")),
                        Integer.parseInt(properties.getProperty("inmemory.send.timeout.seconds")))
        );
    }
}