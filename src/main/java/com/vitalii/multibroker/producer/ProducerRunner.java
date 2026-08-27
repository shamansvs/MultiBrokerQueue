package com.vitalii.multibroker.producer;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.model.PoisonPill;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public final class ProducerRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerRunner.class);

    private final MessageProducer producer;
    private final MessageBroker broker;
    private final String queueName;
    private final long messagesCount;
    private final int consumersCount;

    public ProducerRunner(MessageProducer producer, MessageBroker broker, String queueName,
                          long messagesCount, int consumersCount) {
        this.producer = producer;
        this.broker = broker;
        this.queueName = queueName;
        this.messagesCount = messagesCount;
        this.consumersCount = consumersCount;
    }

    public void run() {
        long start = System.nanoTime();

        try {
            producer.generateAndSend(messagesCount);
        } finally {
            sendPoisonPills();
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - start);
            LOGGER.info("Producer time: {} ms", elapsedMillis);
        }
    }

    private void sendPoisonPills() {
        for (int i = 0; i < consumersCount; i++) {
            broker.send(queueName, PoisonPill.STOP);
        }
    }
}