package com.vitalii.multibroker.producer;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.generator.MessageGenerator;
import com.vitalii.multibroker.model.PojoMessage;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MessageProducerTest {
    private static final String QUEUE_NAME = "test-queue";

    @Test
    void shouldGenerateAndSendRequestedNumberOfMessages() {
        MessageGenerator generator = mock(MessageGenerator.class);
        MessageBroker broker = mock(MessageBroker.class);

        MessageProducer producer = new MessageProducer(
                generator,
                broker,
                QUEUE_NAME
        );

        PojoMessage message = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.now()
        );

        when(generator.generate()).thenReturn(message);

        producer.generateAndSend(3);

        verify(generator, times(3)).generate();
        verify(broker, times(3)).send(QUEUE_NAME, message);
    }
}