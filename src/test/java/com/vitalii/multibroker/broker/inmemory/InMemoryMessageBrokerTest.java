package com.vitalii.multibroker.broker.inmemory;

import com.vitalii.multibroker.consumer.ConsumerWorker;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.processing.MessageProcessor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class InMemoryMessageBrokerTest {
    private static final String QUEUE_NAME = "test-queue";

    @Test
    void shouldDeliverMessageAndStopAfterPoisonPill() throws InterruptedException {

        try (InMemoryMessageBroker broker =
                     new InMemoryMessageBroker(new InMemoryConfig(100, 1))) {
            List<QueueMessage> receivedMessages = new CopyOnWriteArrayList<>();
            CountDownLatch receivedLatch = new CountDownLatch(2);

            broker.subscribe(QUEUE_NAME, message -> {
                receivedMessages.add(message);
                receivedLatch.countDown();

                return message != PoisonPill.STOP;
            });

            PojoMessage message = new PojoMessage(
                    "anastasia",
                    "2000010100019",
                    10,
                    LocalDateTime.now()
            );

            broker.send(QUEUE_NAME, message);
            broker.send(QUEUE_NAME, PoisonPill.STOP);

            boolean wereMessagesReceived = receivedLatch.await(1, TimeUnit.SECONDS);

            assertTrue(wereMessagesReceived);
            assertEquals(List.of(message, PoisonPill.STOP), receivedMessages);
        }
    }

    @Test
    void shouldCloseWithoutPoisonPill() {
        assertTimeoutPreemptively(Duration.ofSeconds(3), () -> {
            CountDownLatch messageHandled = new CountDownLatch(1);

            try (InMemoryMessageBroker broker =
                         new InMemoryMessageBroker(new InMemoryConfig(100, 1))) {
                broker.subscribe(QUEUE_NAME, message -> {
                    messageHandled.countDown();
                    return true;
                });

                PojoMessage message = new PojoMessage(
                        "anastasia",
                        "2000010100019",
                        10,
                        LocalDateTime.of(2026, Month.JANUARY, 1, 12, 0)
                );

                broker.send(QUEUE_NAME, message);

                assertTrue(messageHandled.await(1, TimeUnit.SECONDS),
                        "Consumer did not handle the message");
            }
        });
    }

    @Test
    void shouldReportProcessingFailureWithoutPoisonPill() {
        MessageProcessor processor = mock(MessageProcessor.class);
        ConsumerWorker worker = new ConsumerWorker(processor);

        PojoMessage message = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.of(2026, Month.JANUARY, 15, 10, 30)
        );
        RuntimeException cause = new IllegalStateException("CSV writing failed");
        doThrow(cause).when(processor).process(message);

        try (InMemoryMessageBroker broker =
                     new InMemoryMessageBroker(new InMemoryConfig(100, 1))) {
            broker.subscribe(QUEUE_NAME, worker);
            broker.send(QUEUE_NAME, message);

            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                IllegalStateException exception = assertThrows(
                        IllegalStateException.class,
                        worker::awaitCompletion
                );

                assertSame(cause, exception.getCause());
            });

            verify(processor).process(message);
            assertEquals(0, worker.getProcessedMessagesCount());
        }
    }

    @Test
    void shouldFailWhenQueueRemainsFull() {
        InMemoryConfig config = new InMemoryConfig(1, 1);
        Duration testTimeout = Duration.ofSeconds(5);

        PojoMessage message = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.of(2026, Month.JANUARY, 15, 10, 30)
        );

        try (InMemoryMessageBroker broker = new InMemoryMessageBroker(config)) {
            broker.send(QUEUE_NAME, message);

            IllegalStateException exception = assertTimeoutPreemptively(testTimeout,
                    () -> assertThrows(IllegalStateException.class, () -> broker.send(QUEUE_NAME, message)));

            assertEquals("Timed out waiting for space in queue: " + QUEUE_NAME, exception.getMessage());
        }
    }
}