package com.vitalii.multibroker.consumer;

import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.processing.MessageProcessor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConsumerWorkerTest {
    @Test
    void shouldProcessRegularMessage() {
        MessageProcessor processor = mock(MessageProcessor.class);
        ConsumerWorker worker = new ConsumerWorker(processor);

        PojoMessage message = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.now()
        );

        boolean shouldContinue = worker.handle(message);

        assertTrue(shouldContinue);
        assertEquals(1, worker.getProcessedMessagesCount());
        verify(processor).process(message);
    }

    @Test
    void shouldStopAfterPoisonPill() {
        MessageProcessor processor = mock(MessageProcessor.class);
        ConsumerWorker worker = new ConsumerWorker(processor);

        boolean shouldContinue = worker.handle(PoisonPill.STOP);

        assertFalse(shouldContinue);
        assertEquals(0, worker.getProcessedMessagesCount());
        verifyNoInteractions(processor);

        assertTimeout(
                Duration.ofSeconds(1),
                worker::awaitCompletion
        );
    }
}