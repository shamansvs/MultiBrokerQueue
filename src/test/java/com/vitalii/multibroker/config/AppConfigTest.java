package com.vitalii.multibroker.config;

import com.vitalii.multibroker.broker.activemq.ActiveMqConfig;
import com.vitalii.multibroker.broker.kafka.KafkaConfig;
import com.vitalii.multibroker.broker.rabbitmq.RabbitMqConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class AppConfigTest {
    private Properties properties;

    @BeforeEach
    void setUp() {
        properties = new Properties();

        properties.setProperty("broker.type", "rabbitmq");
        properties.setProperty("queue.name", "test-messages");
        properties.setProperty("consumers.count", "3");
        properties.setProperty("messages.count", "1000000");
        properties.setProperty("csv.valid.path", "valid.csv");
        properties.setProperty("csv.invalid.path", "invalid.csv");

        properties.setProperty("rabbitmq.host", "localhost");
        properties.setProperty("rabbitmq.port", "5672");
        properties.setProperty("rabbitmq.username", "guest");
        properties.setProperty("rabbitmq.passwordd", "rabbit-test-password");

        properties.setProperty("activemq.host", "localhost");
        properties.setProperty("activemq.port", "61616");
        properties.setProperty("activemq.username", "artemis");
        properties.setProperty("activemq.passwordd", "artemis-test-password");

        properties.setProperty("kafka.bootstrap.servers", "localhost:9092");
        properties.setProperty("kafka.group.id", "test-group");
        properties.setProperty("kafka.partitions.count", "3");

        properties.setProperty("consumers.completion.timeout.seconds", "60");
    }

    @Test
    void shouldCreateConfigFromProperties() {
        AppConfig config = AppConfig.fromProperties(properties);

        assertAll(
                () -> assertEquals("rabbitmq", config.brokerType()),
                () -> assertEquals("test-messages", config.queueName()),
                () -> assertEquals(3, config.consumersCount()),
                () -> assertEquals(1_000_000L, config.messagesCount()),
                () -> assertEquals(Path.of("valid.csv"), config.validCsvPath()),
                () -> assertEquals(Path.of("invalid.csv"), config.invalidCsvPath()),
                () -> assertEquals(new RabbitMqConfig(
                        "localhost", 5672,
                        "guest", "rabbit-test-password"), config.rabbitMqConfig()),
                () -> assertEquals(new ActiveMqConfig(
                        "localhost", 61616,
                        "artemis", "artemis-test-password"), config.activeMqConfig()),
                () -> assertEquals(new KafkaConfig("localhost:9092",
                        "test-group", 3), config.kafkaConfig()),
                () -> assertEquals(60, config.consumersCompletionTimeoutSeconds())
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc", "", "1.5", "2147483648"})
    void shouldRejectInvalidConsumersCount(String value) {
        properties.setProperty("consumers.count", value);
        assertThrows(NumberFormatException.class, () -> AppConfig.fromProperties(properties));
    }

    @ParameterizedTest
    @CsvSource({
            "consumers.count, 0",
            "consumers.count, -1",
            "messages.count, 0",
            "messages.count, -1",
            "consumers.completion.timeout.seconds, 0",
            "consumers.completion.timeout.seconds, -1"
    })
    void shouldRejectNonPositiveCounts(String propertyName, String value) {
        properties.setProperty(propertyName, value);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class, () -> AppConfig.fromProperties(properties));

        assertEquals(propertyName + " must be greater than zero", exception.getMessage());
    }
}