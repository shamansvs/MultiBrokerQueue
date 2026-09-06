package com.vitalii.multibroker.broker.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public final class RabbitMqConnectionProvider {
    private final RabbitMqConfig config;

    public RabbitMqConnectionProvider(RabbitMqConfig config) {
        this.config = config;
    }

    public Connection createConnection() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(config.host());
            factory.setPort(config.port());
            factory.setUsername(config.username());
            factory.setPassword(config.password());

            return factory.newConnection();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect to RabbitMQ", e);
        }
    }
}
