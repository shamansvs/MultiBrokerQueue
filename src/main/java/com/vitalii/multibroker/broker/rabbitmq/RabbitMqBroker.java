package com.vitalii.multibroker.broker.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.serialization.QueueMessageSerializer;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class RabbitMqBroker implements MessageBroker {
    private final Connection connection;
    private final QueueMessageSerializer serializer;
    private final ThreadLocal<Channel> producerChannels;
    private final Set<String> declaredQueues = ConcurrentHashMap.newKeySet();
    private final Object queueLock = new Object();

    public RabbitMqBroker(RabbitMqConnectionProvider connectionProvider,
                          QueueMessageSerializer serializer) {
        this.connection = connectionProvider.createConnection();
        this.serializer = serializer;
        this.producerChannels = ThreadLocal.withInitial(this::createChannel);
    }

    @Override
    public void send(String queueName, QueueMessage message) {
        try {
            Channel channel = producerChannels.get();

            ensureQueue(channel, queueName);

            byte[] body = serializer.serialize(message);
            channel.basicPublish("", queueName, null, body);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to send message to RabbitMQ", e);
        }
    }

    @Override
    public void subscribe(String queueName, QueueMessageHandler handler) {
        try {
            Channel channel = createChannel();
            ensureQueue(channel, queueName);

            channel.basicConsume(queueName, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(
                        String consumerTag,
                        Envelope envelope,
                        com.rabbitmq.client.AMQP.BasicProperties properties,
                        byte[] body
                ) throws IOException {
                    QueueMessage message = serializer.deserialize(body);
                    boolean shouldContinue = handler.handle(message);

                    if (!shouldContinue) {
                        channel.basicCancel(consumerTag);
                    }
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to subscribe to RabbitMQ queue: " + queueName, e);
        }

    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close RabbitMQ connection", e);
        }
    }

    private Channel createChannel() {
        try {
            return connection.createChannel();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create RabbitMQ channel", e);
        }
    }

    private void ensureQueue(Channel channel, String queueName) throws IOException {
        if (declaredQueues.contains(queueName)) {
            return;
        }

        synchronized (queueLock) {
            if (declaredQueues.add(queueName)) {
                channel.queueDeclare(queueName, true, false, false, null);
            }
        }
    }
}
