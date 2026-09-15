package com.vitalii.multibroker.broker.rabbitmq;

import com.rabbitmq.client.*;
import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.serialization.QueueMessageSerializer;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class RabbitMqBroker implements MessageBroker {
    private final Connection connection;
    private final QueueMessageSerializer serializer;
    private final ThreadLocal<Channel> producerChannels;
    private final Set<String> declaredQueues = ConcurrentHashMap.newKeySet();
    private final Object queueLock = new Object();
    private final ExecutorService consumerExecutor;
    private volatile boolean closing;

    public RabbitMqBroker(RabbitMqConnectionProvider connectionProvider, QueueMessageSerializer serializer) {
        Objects.requireNonNull(connectionProvider);
        this.serializer = Objects.requireNonNull(serializer);
        this.consumerExecutor = Executors.newCachedThreadPool();

        try {
            this.connection = connectionProvider.createConnection(consumerExecutor);
        } catch (RuntimeException e) {
            consumerExecutor.close();
            throw e;
        }

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
        Objects.requireNonNull(handler);

        if (closing) {
            throw new IllegalStateException("RabbitMQ broker is closing");
        }
        Channel channel = createChannel();

        try {
            ensureQueue(channel, queueName);
            channel.basicQos(1);
            channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
                private boolean stopped;

                @Override
                public void handleDelivery(
                        String consumerTag,
                        Envelope envelope,
                        AMQP.BasicProperties properties,
                        byte[] body
                ) {
                    if (stopped || closing) {
                        return;
                    }

                    long deliveryTag = envelope.getDeliveryTag();
                    try {
                        QueueMessage message = serializer.deserialize(body);

                        if (message == PoisonPill.STOP) {
                            stopped = true;
                            channel.basicCancel(consumerTag);
                            channel.basicAck(deliveryTag, false);
                            handler.handle(message);
                            return;
                        }

                        boolean shouldContinue = handler.handle(message);

                        if (!shouldContinue) {
                            stopped = true;
                            channel.basicCancel(consumerTag);
                        }

                        channel.basicAck(deliveryTag, false);
                    } catch (IOException | RuntimeException e) {
                        stopped = true;

                        abortChannel(channel, e);
                        handler.onError(e);
                    }
                }

                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException signal) {
                    if (!stopped && !closing) {
                        stopped = true;
                        handler.onError(signal);
                    }
                }

                @Override
                public void handleCancel(String consumerTag) {
                    if (!stopped && !closing) {
                        stopped = true;

                        handler.onError(new IllegalStateException("RabbitMQ cancelled consumer: " + consumerTag));
                    }
                }
            });
        } catch (IOException | RuntimeException e) {
            abortChannel(channel, e);

            throw new IllegalStateException("Failed to subscribe to RabbitMQ queue: " + queueName, e);
        }
    }

    @Override
    public synchronized void close() {
        if (closing) {
            return;
        }
        closing = true;

        try (consumerExecutor) {
            try {
                connection.close();
            } catch (AlreadyClosedException ignored) {
            }
        } catch (IOException e) {
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
            if (!declaredQueues.contains(queueName)) {
                channel.queueDeclare(queueName, true, false, false, null);
                declaredQueues.add(queueName);
            }
        }
    }

    private void abortChannel(Channel channel, Throwable originalError) {
        try {
            channel.abort();
        } catch (IOException | RuntimeException closingError) {
            if (closingError != originalError) {
                originalError.addSuppressed(closingError);
            }
        }
    }
}
