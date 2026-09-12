package com.vitalii.multibroker.broker;

import com.vitalii.multibroker.model.QueueMessage;

@FunctionalInterface
public interface QueueMessageHandler {
    boolean handle(QueueMessage message);
    default void onError(Throwable error) {
    }
}
