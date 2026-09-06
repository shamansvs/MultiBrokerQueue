package com.vitalii.multibroker.consumer;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.processing.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class ConsumerRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunner.class);

    private final MessageBroker broker;
    private final MessageProcessor processor;
    private final String queueName;
    private final int consumersCount;
    private final List<ConsumerWorker> consumers = new ArrayList<>();

    private long consumerStartNanos;

    public ConsumerRunner(MessageBroker broker, MessageProcessor processor,
                          String queueName, int consumersCount) {
        this.broker = broker;
        this.processor = processor;
        this.queueName = queueName;
        this.consumersCount = consumersCount;
    }

    public void start() {
        consumerStartNanos = System.nanoTime();

        for (int i = 0; i < consumersCount; i++) {
            ConsumerWorker consumer = new ConsumerWorker(processor);
            consumers.add(consumer);
            broker.subscribe(queueName, consumer);
        }
    }

    public void awaitCompletion() {
        try {
            for (ConsumerWorker consumer : consumers) {
                consumer.awaitCompletion();
            }

            long processedMessagesCount = consumers.stream()
                    .mapToLong(ConsumerWorker::getProcessedMessagesCount)
                    .sum();
            long durationNanos = System.nanoTime() - consumerStartNanos;
            long durationMillis = TimeUnit.NANOSECONDS.toMillis(durationNanos);
            long messagesPerSecond = Math.round(processedMessagesCount * 1_000_000_000.0 / Math.max(1, durationNanos));
            LOGGER.info("Consumers processed {} messages in {} ms ({} msg/s)",
                    processedMessagesCount, durationMillis, messagesPerSecond);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException("Consumer processing was interrupted", e);
        }
    }
}
