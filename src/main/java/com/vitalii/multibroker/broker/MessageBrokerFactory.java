package com.vitalii.multibroker.broker;

import com.vitalii.multibroker.broker.inmemory.InMemoryMessageBroker;
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
                RabbitMqConnectionProvider connectionProvider =
                        new RabbitMqConnectionProvider(config.rabbitMqConfig());

                QueueMessageSerializer serializer =
                        new JsonQueueMessageSerializer();

                yield new RabbitMqBroker(connectionProvider, serializer);
            }

            default -> throw new IllegalArgumentException(
                    "Unsupported broker type: " + config.brokerType()
            );
        };
    }
}
