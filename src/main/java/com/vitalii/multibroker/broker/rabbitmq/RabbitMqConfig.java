package com.vitalii.multibroker.broker.rabbitmq;

public record RabbitMqConfig(
        String host,
        int port,
        String username,
        String password
) {
}
