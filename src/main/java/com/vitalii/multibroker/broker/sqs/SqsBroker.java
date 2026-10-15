package com.vitalii.multibroker.broker.sqs;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.serialization.QueueMessageSerializer;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SqsBroker implements MessageBroker {
    private final SqsClient client;
    private final SqsConfig config;
    private final QueueMessageSerializer serializer;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public SqsBroker(SqsClient client, SqsConfig config, QueueMessageSerializer serializer) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
    }

    @Override
    public void send(String queueName, QueueMessage message) {
        ensureOpen();

        String body = new String(serializer.serialize(message), StandardCharsets.UTF_8);
        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(config.queueUrl())
                .messageBody(body)
                .messageGroupId(config.messageGroupId())
                .messageDeduplicationId(UUID.randomUUID().toString())
                .build();

        try {
            client.sendMessage(request);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to send message to Amazon SQS", e);
        }
    }

    @Override
    public void subscribe(String queueName, QueueMessageHandler handler) {
        ensureOpen();
        Objects.requireNonNull(handler, "handler must not be null");

        if (!subscribed.compareAndSet(false, true)) {
            throw new IllegalStateException("SQS broker supports only one subscription");
        }
        executor.execute(() -> consumeMessages(handler));
    }

    private void consumeMessages(QueueMessageHandler handler) {
        try {
            while (!closed.get() && !Thread.currentThread().isInterrupted()) {
                ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                        .queueUrl(config.queueUrl())
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20)
                        .visibilityTimeout(60)
                        .receiveRequestAttemptId(UUID.randomUUID().toString())
                        .build();
                var response = client.receiveMessage(request);

                for (Message message : response.messages()) {
                    if (closed.get() || Thread.currentThread().isInterrupted()) {
                        return;
                    }
                    if (!handleMessage(message, handler)) {
                        return;
                    }
                }
            }
        } catch (RuntimeException e) {
            if (!closed.get()) {
                handler.onError(e);
            }
        }
    }

    private boolean handleMessage(Message message, QueueMessageHandler handler) {
        QueueMessage queueMessage = serializer.deserialize(message.body().getBytes(StandardCharsets.UTF_8));

        if (queueMessage == PoisonPill.STOP) {
            deleteMessage(message);
            handler.handle(queueMessage);
            return false;
        }

        boolean shouldContinue = handler.handle(queueMessage);
        deleteMessage(message);

        return shouldContinue;
    }

    private void deleteMessage(Message message) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(config.queueUrl())
                .receiptHandle(message.receiptHandle())
                .build();

        client.deleteMessage(request);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("SQS broker is closed");
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        executor.shutdownNow();
        try {
            executor.close();
        } finally {
            client.close();
        }
    }
}