package com.vitalii.multibroker.broker.activemq;

import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.serialization.JsonQueueMessageSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "RUN_ARTEMIS_TESTS", matches = "true")
class ActiveMqBrokerFailureTest {
    private static final ActiveMqConfig CONFIG = new ActiveMqConfig(
            "localhost",
            61616,
            "artemis",
            "artemis"
    );

    @Test
    void shouldRedeliverMessageAfterProcessingFailure() throws InterruptedException {
        String queueName = "test-redelivery-" + UUID.randomUUID();
        PojoMessage message = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.of(2026, Month.JANUARY, 15, 10, 30)
        );
        RuntimeException processingFailure = new IllegalStateException("CSV writing failed");
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        CountDownLatch failureReported = new CountDownLatch(1);

        try (ActiveMqBroker broker = createBroker()) {
            broker.subscribe(queueName, new QueueMessageHandler() {
                @Override
                public boolean handle(QueueMessage receivedMessage) {
                    throw processingFailure;
                }

                @Override
                public void onError(Throwable error) {
                    firstError.compareAndSet(null, error);
                    failureReported.countDown();
                }
            });
            broker.send(queueName, message);
            assertTrue(failureReported.await(10, TimeUnit.SECONDS),
                    "Processing failure was not reported");
        }

        assertSame(processingFailure, firstError.get());
        List<QueueMessage> receivedMessages = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> secondError = new AtomicReference<>();
        CountDownLatch messageReceived = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);

        try (ActiveMqBroker broker = createBroker()) {
            broker.subscribe(queueName, new QueueMessageHandler() {
                @Override
                public boolean handle(QueueMessage receivedMessage) {
                    receivedMessages.add(receivedMessage);
                    if (receivedMessage == PoisonPill.STOP) {
                        completed.countDown();
                        return false;
                    }
                    messageReceived.countDown();
                    return true;
                }

                @Override
                public void onError(Throwable error) {
                    secondError.compareAndSet(null, error);
                    messageReceived.countDown();
                    completed.countDown();
                }
            });

            assertTrue(messageReceived.await(10, TimeUnit.SECONDS),
                    "Unacknowledged message was not redelivered");
            assertNull(secondError.get(), () -> "Redelivery failed: " + secondError.get());
            broker.send(queueName, PoisonPill.STOP);
            assertTrue(completed.await(10, TimeUnit.SECONDS), "Consumer did not stop");
        }

        assertNull(secondError.get(), () -> "Processing failed: " + secondError.get());
        assertEquals(List.of(message, PoisonPill.STOP), receivedMessages);
    }

    private ActiveMqBroker createBroker() {
        return new ActiveMqBroker(new ActiveMqConnectionProvider(CONFIG), new JsonQueueMessageSerializer());
    }
}