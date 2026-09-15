package com.vitalii.multibroker.consumer;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.processing.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ConsumerRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunner.class);

    private final MessageBroker broker;
    private final MessageProcessor processor;
    private final String queueName;
    private final int consumersCount;

    private final List<ConsumerWorker> consumers = new ArrayList<>();
    private final BlockingQueue<ConsumerWorker> completedConsumers = new LinkedBlockingQueue<>();

    private long consumerStartNanos;
    private boolean started;

    public ConsumerRunner(MessageBroker broker, MessageProcessor processor, String queueName, int consumersCount) {
        if (consumersCount < 1) {
            throw new IllegalArgumentException("Consumers count must be at least 1");
        }

        this.broker = Objects.requireNonNull(broker);
        this.processor = Objects.requireNonNull(processor);
        this.queueName = Objects.requireNonNull(queueName);
        this.consumersCount = consumersCount;
    }

    public void start() {
        if (started) {
            throw new IllegalStateException("Consumer runner has already been started");
        }

        started = true;
        consumerStartNanos = System.nanoTime();

        for (int i = 0; i < consumersCount; i++) {
            ConsumerWorker consumer = new ConsumerWorker(processor);
            consumers.add(consumer);

            broker.subscribe(queueName, createHandler(consumer));
        }
    }

    private QueueMessageHandler createHandler(ConsumerWorker consumer) {
        return new QueueMessageHandler() {
            private final AtomicBoolean completionReported = new AtomicBoolean();

            @Override
            public boolean handle(QueueMessage message) {
                try {
                    boolean shouldContinue = consumer.handle(message);

                    if (!shouldContinue) {
                        reportCompletion();
                    }

                    return shouldContinue;
                } catch (RuntimeException e) {
                    onError(e);
                    throw e;
                }
            }

            @Override
            public void onError(Throwable error) {
                consumer.onError(error);
                reportCompletion();
            }

            private void reportCompletion() {
                if (completionReported.compareAndSet(false, true)) {
                    completedConsumers.add(consumer);
                }
            }
        };
    }

    public void awaitCompletion() {
        if (!started) {
            throw new IllegalStateException("Consumer runner has not been started");
        }

        try {
            for (int i = 0; i < consumersCount; i++) {
                ConsumerWorker consumer = completedConsumers.take();
                consumer.awaitCompletion();
            }

            logStatistics();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException("Consumer processing was interrupted", e);
        }
    }

    private void logStatistics() {
        long processedMessagesCount = consumers.stream()
                .mapToLong(ConsumerWorker::getProcessedMessagesCount)
                .sum();

        long durationNanos = System.nanoTime() - consumerStartNanos;
        long durationMillis = TimeUnit.NANOSECONDS.toMillis(durationNanos);

        long messagesPerSecond = Math.round(processedMessagesCount * 1_000_000_000.0 / Math.max(1, durationNanos));

        LOGGER.info("Consumers processed {} messages in {} ms ({} msg/s)",
                processedMessagesCount, durationMillis, messagesPerSecond);
    }
}