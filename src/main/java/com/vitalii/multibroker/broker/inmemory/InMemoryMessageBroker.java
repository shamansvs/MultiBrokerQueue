package com.vitalii.multibroker.broker.inmemory;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.QueueMessage;

import java.util.concurrent.*;


public final class InMemoryMessageBroker implements MessageBroker {
    private final ConcurrentHashMap<String, BlockingQueue<QueueMessage>> queues =
            new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Override
    public void send(String queueName, QueueMessage message) {
        getQueue(queueName).add(message);
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
                    }
                }
        );
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private BlockingQueue<QueueMessage> getQueue(String queueName) {
        return queues.computeIfAbsent(
                queueName, ignored -> new LinkedBlockingQueue<>()
        );
    }
}
