package com.vitalii.multibroker.broker.activemq;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public final class ActiveMqConnectionProvider implements AutoCloseable {
    private final ActiveMQConnectionFactory connectionFactory;
    private final ActiveMqConfig config;

    public ActiveMqConnectionProvider(ActiveMqConfig config) {
        this.config = config;

        String brokerUrl = "tcp://%s:%d".formatted(
                config.host(),
                config.port()
        );

        this.connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
    }

    public Connection createConnection() {
        try {
            return connectionFactory.createConnection(
                    config.username(),
                    config.password()
            );
        } catch (JMSException e) {
            throw new IllegalStateException("Failed to connect to ActiveMQ Artemis", e);
        }
    }

    @Override
    public void close() {
        connectionFactory.close();
    }
}