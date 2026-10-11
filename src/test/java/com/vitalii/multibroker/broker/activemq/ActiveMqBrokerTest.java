package com.vitalii.multibroker.broker.activemq;

import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.serialization.JsonQueueMessageSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "RUN_ARTEMIS_TESTS", matches = "true")
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
        List<QueueMessage> receivedMessages = new CopyOnWriteArrayList<>();
        CountDownLatch completion = new CountDownLatch(1);
        AtomicReference<Throwable> processingError = new AtomicReference<>();

        try (ActiveMqBroker broker = new ActiveMqBroker(new ActiveMqConnectionProvider(config),
                new JsonQueueMessageSerializer())) {
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
            assertTrue(completion.await(5, TimeUnit.SECONDS),
                    "Consumer did not finish within 5 seconds");
        }

        assertNull(processingError.get(), () -> "Processing failed: " + processingError.get());
        assertEquals(List.of(message, PoisonPill.STOP), receivedMessages);
    }

    @Test
    void shouldDeliverAllMessagesAndStopEachConsumer() throws InterruptedException {
        int consumersCount = 3;
        int messagesCount = 7;

        ActiveMqConfig config = new ActiveMqConfig(
                "localhost",
                61616,
                "artemis",
                "artemis"
        );

        String queueName = "test-multiple-consumers-" + UUID.randomUUID();
        LocalDateTime createdAt = LocalDateTime.of(2026, Month.JANUARY, 15, 10, 30);
        List<PojoMessage> sentMessages = IntStream.range(0, messagesCount)
                .mapToObj(index -> new PojoMessage(
                        "anastasia-" + index,
                        "2000010100019",
                        10 + index,
                        createdAt
                ))
                .toList();

        List<List<QueueMessage>> messagesByConsumer = new ArrayList<>();
        CountDownLatch completion = new CountDownLatch(consumersCount);
        AtomicReference<Throwable> processingError = new AtomicReference<>();

        try (ActiveMqBroker broker = new ActiveMqBroker(
                new ActiveMqConnectionProvider(config), new JsonQueueMessageSerializer())) {
            for (int i = 0; i < consumersCount; i++) {
                List<QueueMessage> receivedMessages = new CopyOnWriteArrayList<>();
                messagesByConsumer.add(receivedMessages);
                broker.subscribe(queueName, new QueueMessageHandler() {
                    @Override
                    public boolean handle(QueueMessage message) {
                        receivedMessages.add(message);

                        if (message == PoisonPill.STOP) {
                            completion.countDown();
                            return false;
                        }
                        return true;
                    }

                    @Override
                    public void onError(Throwable error) {
                        processingError.compareAndSet(null, error);
                        while (completion.getCount() > 0) {
                            completion.countDown();
                        }
                    }
                });
            }

            for (PojoMessage message : sentMessages) {
                broker.send(queueName, message);
            }

            for (int i = 0; i < consumersCount; i++) {
                broker.send(queueName, PoisonPill.STOP);
            }

            assertTrue(completion.await(10, TimeUnit.SECONDS),
                    "Not all consumers finished within 10 seconds");
            assertNull(processingError.get(), () -> "Processing failed: " + processingError.get());
        }

        assertNull(processingError.get(), () -> "Processing failed: " + processingError.get());
        for (List<QueueMessage> receivedMessages : messagesByConsumer) {
            assertFalse(receivedMessages.isEmpty(), "Each consumer must receive STOP");
            assertEquals(PoisonPill.STOP, receivedMessages.getLast(),
                    "STOP must be the last message");
            long stopCount = receivedMessages.stream()
                    .filter(message -> message == PoisonPill.STOP)
                    .count();

            assertEquals(1L, stopCount, "Each consumer must receive exactly one STOP");
        }

        List<PojoMessage> processedMessages = messagesByConsumer.stream()
                .flatMap(List::stream)
                .filter(PojoMessage.class::isInstance)
                .map(PojoMessage.class::cast)
                .toList();

        assertEquals(messagesCount, processedMessages.size(),
                "Unexpected number of processed messages");
        assertEquals(new HashSet<>(sentMessages), new HashSet<>(processedMessages),
                "Processed messages must match sent messages");
    }

    @Test
    void shouldWaitForActiveHandlerBeforeClosing() {
        assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
            ActiveMqConfig config = new ActiveMqConfig(
                    "localhost",
                    61616,
                    "artemis",
                    "artemis"
            );
            String queueName = "test-close-" + UUID.randomUUID();
            CountDownLatch handlerStarted = new CountDownLatch(1);
            CountDownLatch allowHandlerToFinish = new CountDownLatch(1);
            CountDownLatch handlerFinished = new CountDownLatch(1);
            CountDownLatch closeStarted = new CountDownLatch(1);
            AtomicReference<Throwable> processingError = new AtomicReference<>();

            try (ActiveMqBroker broker = new ActiveMqBroker(
                    new ActiveMqConnectionProvider(config), new JsonQueueMessageSerializer());
                 ExecutorService executor = Executors.newSingleThreadExecutor()) {
                try {
                    broker.subscribe(queueName, new QueueMessageHandler() {
                        @Override
                        public boolean handle(QueueMessage message) {
                            handlerStarted.countDown();
                            try {
                                allowHandlerToFinish.await();
                                return true;
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("Handler was interrupted", e);
                            } finally {
                                handlerFinished.countDown();
                            }
                        }

                        @Override
                        public void onError(Throwable error) {
                            processingError.compareAndSet(null, error);
                        }
                    });

                    broker.send(queueName, new PojoMessage(
                            "anastasia",
                            "2000010100019",
                            10,
                            LocalDateTime.of(2026, Month.JANUARY, 15, 10, 30)
                    ));

                    assertTrue(handlerStarted.await(5, TimeUnit.SECONDS), "Handler did not start");
                    Future<?> closingTask = executor.submit(() -> {
                        closeStarted.countDown();
                        broker.close();
                    });
                    assertTrue(closeStarted.await(2, TimeUnit.SECONDS), "Closing task did not start");
                    assertThrows(TimeoutException.class, () -> closingTask.get(200, TimeUnit.MILLISECONDS));

                    allowHandlerToFinish.countDown();
                    closingTask.get(5, TimeUnit.SECONDS);

                    assertEquals(0L, handlerFinished.getCount());
                    assertNull(processingError.get(), () -> "Processing failed: " + processingError.get());
                } finally {
                    allowHandlerToFinish.countDown();
                }
            }
        });
    }
}