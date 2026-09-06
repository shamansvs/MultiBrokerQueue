package com.vitalii.multibroker.broker;

import com.vitalii.multibroker.model.QueueMessage;

public interface MessageBroker extends AutoCloseable {
    void send(String queueName, QueueMessage message);

    void subscribe(String queueName, QueueMessageHandler handler);

    @Override
    void close();
}
