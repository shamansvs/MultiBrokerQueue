package com.vitalii.multibroker.broker.inmemory;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.model.QueueMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryMessageBroker implements MessageBroker {
    private final ConcurrentHashMap<String, BlockingQueue<QueueMessage>> queues =
            new ConcurrentHashMap<>();

    @Override
    public void send(String queueName, QueueMessage message) {
        getQueue(queueName).add(message);
    }

    @Override
    public QueueMessage receive(String queueName) throws InterruptedException {
        return getQueue(queueName).take();
    }

    private BlockingQueue<QueueMessage> getQueue(String queueName) {
        return queues.computeIfAbsent(
                queueName, ignored -> new LinkedBlockingQueue<>()
        );
    }
}
