package com.vitalii.multibroker.broker.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.serialization.JsonQueueMessageSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfEnvironmentVariable(named = "RUN_RABBITMQ_TESTS", matches = "true")
class RabbitMqBrokerFailureTest {
    private final String queueName = "test-rabbit-failure-" + UUID.randomUUID();

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
    void shouldRedeliverMessageAfterProcessingFailure() throws Exception {
        PojoMessage message = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.of(2026, Month.JANUARY, 1, 12, 0)
        );

        IllegalStateException expectedError = new IllegalStateException("CSV writing failed");
        AtomicReference<Throwable> receivedError = new AtomicReference<>();
        CountDownLatch errorLatch = new CountDownLatch(1);

        QueueMessageHandler failingHandler = new QueueMessageHandler() {
            @Override
            public boolean handle(QueueMessage receivedMessage) {
                throw expectedError;
            }

            @Override
            public void onError(Throwable error) {
                receivedError.compareAndSet(null, error);
                errorLatch.countDown();
            }
        };

        try (RabbitMqBroker broker = createBroker()) {
            broker.subscribe(queueName, failingHandler);
            broker.send(queueName, message);

            assertTrue(errorLatch.await(10, TimeUnit.SECONDS), "Processing error was not reported");
        }

        assertSame(expectedError, receivedError.get());
        assertEquals(1, adminChannel.queueDeclarePassive(queueName).getMessageCount(),
                "Failed message must remain in the queue");

        List<QueueMessage> receivedMessages = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> recoveryError = new AtomicReference<>();
        CountDownLatch completionLatch = new CountDownLatch(1);

        QueueMessageHandler successfulHandler = new QueueMessageHandler() {
            @Override
            public boolean handle(QueueMessage receivedMessage) {
                receivedMessages.add(receivedMessage);

                if (receivedMessage == PoisonPill.STOP) {
                    completionLatch.countDown();
                    return false;
                }

                return true;
            }

            @Override
            public void onError(Throwable error) {
                recoveryError.compareAndSet(null, error);
                completionLatch.countDown();
            }
        };

        try (RabbitMqBroker broker = createBroker()) {
            broker.subscribe(queueName, successfulHandler);

            broker.send(queueName, PoisonPill.STOP);

            assertTrue(completionLatch.await(10, TimeUnit.SECONDS),
                    "Consumer did not finish after redelivery");
        }

        assertNull(recoveryError.get(), () -> "Repeated processing failed: " + recoveryError.get());
        assertEquals(List.of(message, PoisonPill.STOP), receivedMessages);

        assertEquals(0, adminChannel.queueDeclarePassive(queueName).getMessageCount(),
                "Successfully processed messages must be acknowledged");
    }

    private RabbitMqBroker createBroker() {
        return new RabbitMqBroker(connectionProvider, new JsonQueueMessageSerializer());
    }
}