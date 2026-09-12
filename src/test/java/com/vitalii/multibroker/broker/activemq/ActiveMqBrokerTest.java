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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfEnvironmentVariable(
        named = "RUN_ARTEMIS_TESTS",
        matches = "true"
)
class ActiveMqBrokerTest {

    @Test
    void shouldDeliverMessageAndPoisonPill() throws InterruptedException {
        ActiveMqConfig config = new ActiveMqConfig(
                "localhost",
                61616,
                "artemis",
                "artemis"
        );

        String queueName = "test-pojo-" + UUID.randomUUID();

        PojoMessage message = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.of(2026, Month.JANUARY, 15, 10, 30)
        );

        List<QueueMessage> receivedMessages =
                new CopyOnWriteArrayList<>();

        CountDownLatch completion = new CountDownLatch(1);
        AtomicReference<Throwable> processingError =
                new AtomicReference<>();

        try (ActiveMqBroker broker = new ActiveMqBroker(
                new ActiveMqConnectionProvider(config),
                new JsonQueueMessageSerializer()
        )) {
            broker.subscribe(queueName, new QueueMessageHandler() {
                @Override
                public boolean handle(QueueMessage receivedMessage) {
                    receivedMessages.add(receivedMessage);

                    if (receivedMessage == PoisonPill.STOP) {
                        completion.countDown();
                        return false;
                    }

                    return true;
                }

                @Override
                public void onError(Throwable error) {
                    processingError.set(error);
                    completion.countDown();
                }
            });

            broker.send(queueName, message);
            broker.send(queueName, PoisonPill.STOP);

            assertTrue(
                    completion.await(5, TimeUnit.SECONDS),
                    "Consumer did not finish within 5 seconds"
            );
        }

        assertNull(
                processingError.get(),
                () -> "Processing failed: " + processingError.get()
        );

        assertEquals(
                List.of(message, PoisonPill.STOP),
                receivedMessages
        );
    }
}