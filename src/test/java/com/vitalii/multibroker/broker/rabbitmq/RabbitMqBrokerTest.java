package com.vitalii.multibroker.broker.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.serialization.JsonQueueMessageSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

@EnabledIfEnvironmentVariable(named = "RUN_RABBITMQ_TESTS", matches = "true")
class RabbitMqBrokerTest {
    private static final int CONSUMERS_COUNT = 3;

    private final String queueName = "test-rabbit-" + UUID.randomUUID();
    private final RabbitMqConnectionProvider connectionProvider = new RabbitMqConnectionProvider(
            new RabbitMqConfig("localhost", 5672, "guest", "guest"));

    private ExecutorService adminExecutor;
    private Connection adminConnection;
    private Channel adminChannel;

    @BeforeEach
    void setUp() throws Exception {
        adminExecutor = Executors.newCachedThreadPool();
        adminConnection = connectionProvider.createConnection(adminExecutor);
        adminChannel = adminConnection.createChannel();

        adminChannel.queueDeclare(queueName, true, false, false, null);
    }

    @AfterEach
    void tearDown() throws Exception {
        ExecutorService executor = adminExecutor;
        Connection connection = adminConnection;
        Channel channel = adminChannel;

        try (executor; connection; channel) {
            if (channel != null && channel.isOpen()) {
                channel.queueDelete(queueName);
            }
        }
    }

    @Test
    void shouldDeliverAllMessagesAndStopEveryConsumer() throws Exception {
        List<PojoMessage> messages = IntStream.range(0, 7)
                .mapToObj(index -> new PojoMessage(
                        "anastasia" + index,
                        "2000010100019",
                        10 + index,
                        LocalDateTime.of(2026, Month.JANUARY, 1, 12, 0)
                ))
                .toList();

        List<List<QueueMessage>> receivedByConsumer = new ArrayList<>();
        CountDownLatch completionLatch = new CountDownLatch(CONSUMERS_COUNT);
        AtomicReference<Throwable> processingError = new AtomicReference<>();

        try (RabbitMqBroker broker = new RabbitMqBroker(connectionProvider, new JsonQueueMessageSerializer())) {
            for (int i = 0; i < CONSUMERS_COUNT; i++) {
                List<QueueMessage> receivedMessages = new CopyOnWriteArrayList<>();

                receivedByConsumer.add(receivedMessages);
                broker.subscribe(queueName, createHandler(receivedMessages, completionLatch, processingError));
            }

            for (PojoMessage message : messages) {
                broker.send(queueName, message);
            }

            for (int i = 0; i < CONSUMERS_COUNT; i++) {
                broker.send(queueName, PoisonPill.STOP);
            }

            assertTrue(completionLatch.await(10, TimeUnit.SECONDS), "Not all consumers finished");
        }

        assertNull(processingError.get(), () -> "Consumer failed: " + processingError.get());

        for (List<QueueMessage> receivedMessages : receivedByConsumer) {
            assertFalse(receivedMessages.isEmpty(), "Every consumer must receive at least a STOP");
            assertEquals(PoisonPill.STOP, receivedMessages.getLast(),
                    "STOP must be the last received message");

            long stopCount = receivedMessages.stream()
                    .filter(message -> message == PoisonPill.STOP)
                    .count();

            assertEquals(1L, stopCount, "Every consumer must receive exactly one STOP");
        }

        List<QueueMessage> receivedData = receivedByConsumer.stream()
                .flatMap(List::stream)
                .filter(message -> message != PoisonPill.STOP)
                .toList();

        assertEquals(messages.size(), receivedData.size(), "Unexpected number of received messages");
        assertEquals(new HashSet<>(messages), new HashSet<>(receivedData),
                "Received messages must match the sent messages");
        assertEquals(0, adminChannel.queueDeclarePassive(queueName).getMessageCount(),
                "Queue must be empty after successful processing");
    }

    private QueueMessageHandler createHandler(List<QueueMessage> receivedMessages, CountDownLatch completionLatch,
                                              AtomicReference<Throwable> processingError) {
        return new QueueMessageHandler() {
            @Override
            public boolean handle(QueueMessage message) {
                receivedMessages.add(message);

                if (message == PoisonPill.STOP) {
                    completionLatch.countDown();
                    return false;
                }

                return true;
            }

            @Override
            public void onError(Throwable error) {
                processingError.compareAndSet(null, error);
                completionLatch.countDown();
            }
        };
    }

    @Test
    void shouldWaitForActiveHandlerBeforeClosing() {
        assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
            CountDownLatch handlerStarted = new CountDownLatch(1);
            CountDownLatch allowHandlerToFinish = new CountDownLatch(1);
            CountDownLatch handlerFinished = new CountDownLatch(1);
            CountDownLatch closeStarted = new CountDownLatch(1);

            try (RabbitMqBroker broker = new RabbitMqBroker(connectionProvider, new JsonQueueMessageSerializer());
                 ExecutorService closingExecutor = Executors.newSingleThreadExecutor()) {

                try {
                    broker.subscribe(queueName, message -> {
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
                    });

                    PojoMessage message = new PojoMessage(
                            "anastasia",
                            "2000010100019",
                            10,
                            LocalDateTime.of(2026, Month.JANUARY, 1, 12, 0)
                    );

                    broker.send(queueName, message);

                    assertTrue(handlerStarted.await(5, TimeUnit.SECONDS), "Handler did not start");

                    Future<?> closingTask = closingExecutor.submit(() -> {
                        closeStarted.countDown();
                        broker.close();
                    });

                    assertTrue(closeStarted.await(2, TimeUnit.SECONDS), "Closing task did not start");
                    assertThrows(TimeoutException.class, () -> closingTask.get(200, TimeUnit.MILLISECONDS),
                            "Broker must not close while the handler is active");

                    allowHandlerToFinish.countDown();
                    closingTask.get(5, TimeUnit.SECONDS);

                    assertEquals(0L, handlerFinished.getCount(),
                            "Handler must finish before broker.close() returns");
                } finally {
                    allowHandlerToFinish.countDown();
                }
            }
        });
    }
}