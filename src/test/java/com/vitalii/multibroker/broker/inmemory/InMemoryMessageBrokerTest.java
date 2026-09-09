package com.vitalii.multibroker.broker.inmemory;

import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.QueueMessage;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryMessageBrokerTest {
    private static final String QUEUE_NAME = "test-queue";

    @Test
    void shouldDeliverMessageAndStopAfterPoisonPill()
            throws InterruptedException {

        try (InMemoryMessageBroker broker = new InMemoryMessageBroker()) {
            List<QueueMessage> receivedMessages =
                    new CopyOnWriteArrayList<>();

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

            boolean wereMessagesReceived =
                    receivedLatch.await(1, TimeUnit.SECONDS);

            assertTrue(wereMessagesReceived);
            assertEquals(
                    List.of(message, PoisonPill.STOP),
                    receivedMessages
            );
        }
    }
}