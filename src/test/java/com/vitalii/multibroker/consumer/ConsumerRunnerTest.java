package com.vitalii.multibroker.consumer;

import com.vitalii.multibroker.broker.inmemory.InMemoryMessageBroker;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.processing.MessageProcessor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ConsumerRunnerTest {
    private static final String QUEUE_NAME = "test-queue";
    private static final int CONSUMERS_COUNT = 2;

    @Test
    void shouldProcessMessagesAndStopAllConsumers() {
        MessageProcessor processor = mock(MessageProcessor.class);

        try (InMemoryMessageBroker broker = new InMemoryMessageBroker()) {
            ConsumerRunner runner = new ConsumerRunner(
                    broker,
                    processor,
                    QUEUE_NAME,
                    CONSUMERS_COUNT
            );

            runner.start();

            PojoMessage message = new PojoMessage(
                    "anastasia",
                    "2000010100019",
                    10,
                    LocalDateTime.now()
            );

            broker.send(QUEUE_NAME, message);
            broker.send(QUEUE_NAME, message);

            broker.send(QUEUE_NAME, PoisonPill.STOP);
            broker.send(QUEUE_NAME, PoisonPill.STOP);

            assertTimeoutPreemptively(
                    Duration.ofSeconds(2),
                    runner::awaitCompletion
            );

            verify(processor, times(2)).process(message);
        }
    }
}