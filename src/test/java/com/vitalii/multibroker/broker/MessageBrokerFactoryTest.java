package com.vitalii.multibroker.broker;

import com.vitalii.multibroker.broker.inmemory.InMemoryMessageBroker;
import com.vitalii.multibroker.broker.rabbitmq.RabbitMqConfig;
import com.vitalii.multibroker.config.AppConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageBrokerFactoryTest {

    @Test
    void shouldCreateInMemoryBroker() {
        AppConfig config = createConfig("inmemory");

        try (MessageBroker broker = MessageBrokerFactory.create(config)) {
            assertInstanceOf(InMemoryMessageBroker.class, broker);
        }
    }

    @Test
    void shouldThrowExceptionForUnsupportedBrokerType() {
        AppConfig config = createConfig("unknown-broker");

        assertThrows(
                IllegalArgumentException.class,
                () -> MessageBrokerFactory.create(config)
        );
    }

    private AppConfig createConfig(String brokerType) {
        return new AppConfig(
                brokerType,
                "test-queue",
                1,
                1,
                100,
                Path.of("valid.csv"),
                Path.of("invalid.csv"),
                new RabbitMqConfig(
                        "localhost",
                        5672,
                        "guest",
                        "guest"
                )
        );
    }
}