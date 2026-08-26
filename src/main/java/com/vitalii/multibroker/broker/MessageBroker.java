package com.vitalii.multibroker.broker;

import com.vitalii.multibroker.model.QueueMessage;

public interface MessageBroker {
    void send(String queueName, QueueMessage message);
}
