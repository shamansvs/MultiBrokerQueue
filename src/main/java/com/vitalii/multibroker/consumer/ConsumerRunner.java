package com.vitalii.multibroker.consumer;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.processing.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class ConsumerRunner implements AutoCloseable {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ConsumerRunner.class);

    private final MessageBroker broker;
    private final MessageProcessor processor;
    private final String queueName;
    private final int consumersCount;
    private final ExecutorService executor;
    private final List<Future<?>> futures = new ArrayList<>();

    private long startNanos;

    public ConsumerRunner(MessageBroker broker, MessageProcessor processor,
                          String queueName, int consumersCount) {
        this.broker = broker;
        this.processor = processor;
        this.queueName = queueName;
        this.consumersCount = consumersCount;
        this.executor = Executors.newFixedThreadPool(consumersCount);
    }

    public void start() {
        startNanos = System.nanoTime();

        for (int i = 0; i < consumersCount; i++) {
            ConsumerWorker consumer = new ConsumerWorker(broker, processor, queueName);
            futures.add(executor.submit(consumer));
        }
    }

    public void awaitCompletion() {
        try {
            for (Future<?> future : futures) {
                future.get();
            }
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            LOGGER.info("Consumer time: {} ms", elapsedMillis);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Consumer processing was interrupted", e);

        } catch (ExecutionException e) {
            throw new IllegalStateException("Consumer processing failed", e);

        } finally {
            executor.shutdown();
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
