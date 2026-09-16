package com.vitalii.multibroker.producer;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.model.PoisonPill;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
        ProducerRunner runner = new ProducerRunner(producer, broker, QUEUE_NAME, MESSAGES_COUNT, CONSUMERS_COUNT);
        runner.run();

        verify(producer).generateAndSend(MESSAGES_COUNT);
        verify(broker, times(CONSUMERS_COUNT)).send(QUEUE_NAME, PoisonPill.STOP);
    }

    @Test
    void shouldPropagateProducerFailureWithoutSendingPoisonPills() {
        MessageProducer producer = mock(MessageProducer.class);
        MessageBroker broker = mock(MessageBroker.class);
        RuntimeException cause = new IllegalStateException("Failed to send message");

        doThrow(cause).when(producer).generateAndSend(MESSAGES_COUNT);

        ProducerRunner runner = new ProducerRunner(producer, broker, QUEUE_NAME, MESSAGES_COUNT, CONSUMERS_COUNT);
        IllegalStateException exception = assertThrows(IllegalStateException.class, runner::run);

        assertSame(cause, exception);
        verify(producer).generateAndSend(MESSAGES_COUNT);
        verifyNoInteractions(broker);
    }

    @Test
    void shouldPropagatePoisonPillSendingFailure() {
        MessageProducer producer = mock(MessageProducer.class);
        MessageBroker broker = mock(MessageBroker.class);
        RuntimeException cause = new IllegalStateException("Failed to send STOP");

        doThrow(cause).when(broker).send(QUEUE_NAME, PoisonPill.STOP);

        ProducerRunner runner = new ProducerRunner(producer, broker, QUEUE_NAME, MESSAGES_COUNT, CONSUMERS_COUNT);
        IllegalStateException exception = assertThrows(IllegalStateException.class, runner::run);

        assertSame(cause, exception);

        verify(producer).generateAndSend(MESSAGES_COUNT);
        verify(broker).send(QUEUE_NAME, PoisonPill.STOP);
        verifyNoMoreInteractions(broker);
    }
}