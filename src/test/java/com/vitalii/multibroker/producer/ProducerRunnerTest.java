package com.vitalii.multibroker.producer;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.model.PoisonPill;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ProducerRunnerTest {
    private static final String QUEUE_NAME = "test-queue";
    private static final long MESSAGES_COUNT = 100;
    private static final int CONSUMERS_COUNT = 3;

    @Test
    void shouldGenerateMessagesAndSendPoisonPillForEachConsumer() {
        MessageProducer producer = mock(MessageProducer.class);
        MessageBroker broker = mock(MessageBroker.class);

        ProducerRunner runner = new ProducerRunner(
                producer,
                broker,
                QUEUE_NAME,
                MESSAGES_COUNT,
                CONSUMERS_COUNT
        );

        runner.run();

        verify(producer).generateAndSend(MESSAGES_COUNT);

        verify(broker, times(CONSUMERS_COUNT))
                .send(QUEUE_NAME, PoisonPill.STOP);
    }
}