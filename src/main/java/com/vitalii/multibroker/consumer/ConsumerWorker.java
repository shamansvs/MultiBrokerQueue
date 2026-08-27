package com.vitalii.multibroker.consumer;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.processing.MessageProcessor;

public final class ConsumerWorker implements Runnable {
    private final MessageBroker broker;
    private final MessageProcessor processor;
    private final String queueName;

    public ConsumerWorker(
            MessageBroker broker,
            MessageProcessor processor,
            String queueName
    ) {
        this.broker = broker;
        this.processor = processor;
        this.queueName = queueName;
    }

    @Override
    public void run() {
        try {
            while (true) {
                QueueMessage message = broker.receive(queueName);

                if (message == PoisonPill.STOP) {
                    return;
                }

                PojoMessage pojoMessage = (PojoMessage) message;
                processor.process(pojoMessage);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
