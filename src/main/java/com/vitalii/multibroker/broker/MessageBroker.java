package com.vitalii.multibroker.broker;

import com.vitalii.multibroker.model.PojoMessage;

public interface MessageBroker {
    void send(String queueName, PojoMessage message);
}
