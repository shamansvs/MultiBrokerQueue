package com.vitalii.multibroker.broker.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

public final class RabbitMqConnectionProvider {
    private final RabbitMqConfig config;

    public RabbitMqConnectionProvider(RabbitMqConfig config) {
        this.config = Objects.requireNonNull(config);
    }

    public Connection createConnection(ExecutorService consumerExecutor) {
        Objects.requireNonNull(consumerExecutor);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.host());
        factory.setPort(config.port());
        factory.setUsername(config.username());
        factory.setPassword(config.password());

        try {
            return factory.newConnection(consumerExecutor);
        } catch (IOException | TimeoutException e) {
            throw new IllegalStateException("Failed to connect to RabbitMQ", e);
        }
    }
}