package com.vitalii.multibroker.broker;

import com.vitalii.multibroker.broker.activemq.ActiveMqBroker;
import com.vitalii.multibroker.broker.activemq.ActiveMqConnectionProvider;
import com.vitalii.multibroker.broker.inmemory.InMemoryMessageBroker;
import com.vitalii.multibroker.broker.kafka.KafkaBroker;
import com.vitalii.multibroker.broker.kafka.KafkaConfig;
import com.vitalii.multibroker.broker.kafka.KafkaTopicManager;
import com.vitalii.multibroker.broker.rabbitmq.RabbitMqBroker;
import com.vitalii.multibroker.broker.rabbitmq.RabbitMqConnectionProvider;
import com.vitalii.multibroker.config.AppConfig;
import com.vitalii.multibroker.serialization.JsonQueueMessageSerializer;
import com.vitalii.multibroker.serialization.QueueMessageSerializer;

import java.util.Locale;

public final class MessageBrokerFactory {
    private MessageBrokerFactory() {
    }

    public static MessageBroker create(AppConfig config) {
        String brokerType = config.brokerType()
                .trim()
                .toLowerCase(Locale.ROOT);

        return switch (brokerType) {
            case "inmemory" -> new InMemoryMessageBroker();

            case "rabbitmq" -> {
                RabbitMqConnectionProvider connectionProvider = new RabbitMqConnectionProvider(config.rabbitMqConfig());
                QueueMessageSerializer serializer = new JsonQueueMessageSerializer();

                yield new RabbitMqBroker(connectionProvider, serializer);
            }

            case "activemq" -> {
                ActiveMqConnectionProvider connectionProvider = new ActiveMqConnectionProvider(config.activeMqConfig());
                QueueMessageSerializer serializer = new JsonQueueMessageSerializer();

                yield new ActiveMqBroker(connectionProvider, serializer);
            }

            case "kafka" -> createKafkaBroker(config);

            default -> throw new IllegalArgumentException(
                    "Unsupported broker type: " + config.brokerType()
            );
        };
    }

    private static MessageBroker createKafkaBroker(AppConfig config) {
        KafkaConfig kafkaConfig = config.kafkaConfig();

        if (config.producersCount() != 1) {
            throw new IllegalArgumentException("Kafka currently requires one producer");
        }

        if (config.consumersCount() != kafkaConfig.partitionsCount()) {
            throw new IllegalArgumentException("Kafka consumers count must equal partitions count");
        }

        KafkaTopicManager topicManager = new KafkaTopicManager(kafkaConfig);
        topicManager.ensureTopic(config.queueName());

        return new KafkaBroker(kafkaConfig, new JsonQueueMessageSerializer());
    }
}
