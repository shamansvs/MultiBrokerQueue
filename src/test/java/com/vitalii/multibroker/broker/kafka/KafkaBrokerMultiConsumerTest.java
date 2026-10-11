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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfEnvironmentVariable(named = "RUN_KAFKA_TESTS", matches = "true")
class KafkaBrokerMultiConsumerTest {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int PARTITIONS_COUNT = 3;

    private final String topicName = "test-multi-pojo-" + UUID.randomUUID();
    private final String groupId = "test-multi-consumers-" + UUID.randomUUID();

    private Admin admin;

    @BeforeEach
    void setUp() throws Exception {
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS));

        NewTopic topic = new NewTopic(topicName, PARTITIONS_COUNT, (short) 1);

        admin.createTopics(List.of(topic))
                .all()
                .get(10, TimeUnit.SECONDS);
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
    void shouldDistributeMessagesAndStopAllConsumers() throws Exception {
        KafkaConfig config = new KafkaConfig(BOOTSTRAP_SERVERS, groupId, PARTITIONS_COUNT);

        List<PojoMessage> messages = IntStream.range(0, 7)
                .mapToObj(index -> new PojoMessage(
                        "anastasia" + index,
                        "2000010100019",
                        10 + index,
                        LocalDateTime.of(2026, Month.JANUARY, 1, 12, 0)
                ))
                .toList();

        List<List<QueueMessage>> receivedByConsumer = new ArrayList<>();

        CountDownLatch completionLatch = new CountDownLatch(PARTITIONS_COUNT);

        AtomicReference<Throwable> processingError = new AtomicReference<>();

        try (KafkaBroker broker = new KafkaBroker(config, new JsonQueueMessageSerializer())) {
            for (int partition = 0; partition < PARTITIONS_COUNT; partition++) {
                List<QueueMessage> receivedMessages = new CopyOnWriteArrayList<>();

                receivedByConsumer.add(receivedMessages);

                QueueMessageHandler handler = createHandler(receivedMessages, completionLatch, processingError);

                broker.subscribe(topicName, handler);
            }

            for (PojoMessage message : messages) {
                broker.send(topicName, message);
            }

            for (int partition = 0; partition < PARTITIONS_COUNT; partition++) {
                broker.send(topicName, PoisonPill.STOP);
            }

            assertTrue(completionLatch.await(20, TimeUnit.SECONDS),
                    "Not all consumers finished within 20 seconds");
        }

        assertNull(processingError.get(), () -> "Consumer failed: " + processingError.get());

        Map<TopicPartition, OffsetAndMetadata> offsets =
                admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS);

        for (int partition = 0; partition < PARTITIONS_COUNT; partition++) {
            List<QueueMessage> expectedMessages = new ArrayList<>();

            for (int index = partition;
                 index < messages.size();
                 index += PARTITIONS_COUNT) {
                expectedMessages.add(messages.get(index));
            }

            expectedMessages.add(PoisonPill.STOP);

            assertEquals(expectedMessages, receivedByConsumer.get(partition),
                    "Unexpected messages for partition " + partition);

            TopicPartition topicPartition = new TopicPartition(topicName, partition);

            OffsetAndMetadata committedOffset = offsets.get(topicPartition);

            assertNotNull(committedOffset, "Offset was not committed for partition " + partition);

            assertEquals(expectedMessages.size(), committedOffset.offset(),
                    "Unexpected offset for partition " + partition);
        }
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
}