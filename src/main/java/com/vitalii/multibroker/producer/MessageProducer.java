package com.vitalii.multibroker.producer;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.generator.MessageGenerator;

import java.util.Objects;
import java.util.stream.Stream;

public class MessageProducer {
    private final MessageGenerator generator;
    private final MessageBroker broker;
    private final String queueName;

    public MessageProducer(MessageGenerator generator, MessageBroker broker, String queueName) {
        this.generator = Objects.requireNonNull(generator);
        this.broker = Objects.requireNonNull(broker);
        this.queueName = Objects.requireNonNull(queueName);
    }

    public void generateAndSend(long messagesCount) {
        if (messagesCount <= 0) {
            throw new IllegalArgumentException("messagesCount must be greater than zero");
        }
        Stream.generate(generator::generate)
                .limit(messagesCount)
                .forEach(message -> broker.send(queueName, message));
    }
}
