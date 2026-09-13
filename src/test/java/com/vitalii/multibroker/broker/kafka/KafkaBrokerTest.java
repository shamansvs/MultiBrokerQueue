package com.vitalii.multibroker.broker.kafka;

import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.serialization.JsonQueueMessageSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfEnvironmentVariable(named = "RUN_KAFKA_TESTS", matches = "true")
class KafkaBrokerTest {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private final String topicName = "test-pojo-" + UUID.randomUUID();
    private final String groupId = "test-consumers-" + UUID.randomUUID();

    private Admin admin;

    @BeforeEach
    void setUp() throws Exception {
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS));

        NewTopic topic = new NewTopic(topicName, 1, (short) 1);

        admin.createTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (admin == null) {
            return;
        }

        try {
            admin.deleteTopics(List.of(topicName))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        } finally {
            admin.close();
        }
    }

    @Test
    void shouldReceiveMessageAndPoisonPillAndCommitOffset() throws Exception {
        KafkaConfig config = new KafkaConfig(BOOTSTRAP_SERVERS, groupId);

        PojoMessage message = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.of(2026, Month.JANUARY, 1, 12, 0)
        );

        List<QueueMessage> receivedMessages = sendAndReceive(config, message);

        assertEquals(List.of(message, PoisonPill.STOP), receivedMessages);

        assertCommittedOffset(2L);
    }

    @Test
    void shouldResumeFromCommittedOffsetAfterRestart() throws Exception {
        KafkaConfig config = new KafkaConfig(BOOTSTRAP_SERVERS, groupId);

        PojoMessage firstMessage = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.of(2026, Month.JANUARY, 1, 12, 0)
        );

        PojoMessage secondMessage = new PojoMessage(
                "alexander",
                "2000010100019",
                20,
                LocalDateTime.of(2026, Month.JANUARY, 1, 13, 0)
        );

        List<QueueMessage> firstRun = sendAndReceive(config, firstMessage);

        assertEquals(List.of(firstMessage, PoisonPill.STOP), firstRun);

        assertCommittedOffset(2L);

        List<QueueMessage> secondRun = sendAndReceive(config, secondMessage);

        assertEquals(List.of(secondMessage, PoisonPill.STOP), secondRun);

        assertCommittedOffset(4L);
    }

    private List<QueueMessage> sendAndReceive(KafkaConfig config, PojoMessage message) throws InterruptedException {
        List<QueueMessage> receivedMessages = new CopyOnWriteArrayList<>();
        CountDownLatch completionLatch = new CountDownLatch(1);
        AtomicReference<Throwable> processingError = new AtomicReference<>();

        QueueMessageHandler handler = new QueueMessageHandler() {
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
                processingError.compareAndSet(null, error);
                completionLatch.countDown();
            }
        };

        try (KafkaBroker broker = new KafkaBroker(config, new JsonQueueMessageSerializer())) {
            broker.subscribe(topicName, handler);

            broker.send(topicName, message);
            broker.send(topicName, PoisonPill.STOP);

            assertTrue(completionLatch.await(20, TimeUnit.SECONDS),
                    "Consumer did not finish within 20 seconds");
        }

        assertNull(processingError.get(), () -> "Consumer failed: " + processingError.get());

        return List.copyOf(receivedMessages);
    }

    private void assertCommittedOffset(long expectedOffset) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets =
                admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS);

        TopicPartition partition = new TopicPartition(topicName, 0);
        OffsetAndMetadata committedOffset = offsets.get(partition);

        assertNotNull(committedOffset, "Consumer offset was not committed");

        assertEquals(expectedOffset, committedOffset.offset(), "Unexpected committed offset");
    }
}