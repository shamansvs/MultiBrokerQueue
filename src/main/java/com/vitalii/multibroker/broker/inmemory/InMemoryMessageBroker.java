package com.vitalii.multibroker.broker.inmemory;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.QueueMessage;

import java.util.Objects;
import java.util.concurrent.*;


public final class InMemoryMessageBroker implements MessageBroker {
    private final ConcurrentHashMap<String, BlockingQueue<QueueMessage>> queues = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final int capacity;
    private final int sendTimeoutSeconds;

    public InMemoryMessageBroker(InMemoryConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        this.capacity = config.capacity();
        this.sendTimeoutSeconds = config.sendTimeoutSeconds();
    }

    @Override
    public void send(String queueName, QueueMessage message) {
        try {
            boolean sent = getQueue(queueName).offer(message, sendTimeoutSeconds, TimeUnit.SECONDS);

            if (!sent) {
                throw new IllegalStateException("Timed out waiting for space in queue: " + queueName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException("Sending to queue was interrupted: " + queueName, e);
        }
    }

    @Override
    public void subscribe(String queueName, QueueMessageHandler handler) {
        executor.submit(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    QueueMessage message = getQueue(queueName).take();
                    boolean shouldContinue = handler.handle(message);
                    if (!shouldContinue) {
                        return;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!executor.isShutdown()) {
                    handler.onError(e);
                }
            } catch (RuntimeException e) {
                handler.onError(e);
            }
        });
    }

    @Override
    public void close() {
        executor.shutdownNow();
        executor.close();
    }

    private BlockingQueue<QueueMessage> getQueue(String queueName) {
        return queues.computeIfAbsent(queueName, ignored -> new LinkedBlockingQueue<>(capacity));
    }
}
