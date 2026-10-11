package com.vitalii.multibroker.broker.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class KafkaTopicManager {
    private static final int TIMEOUT_SECONDS = 10;
    private static final short REPLICATION_FACTOR = 1;

    private final KafkaConfig config;

    public KafkaTopicManager(KafkaConfig config) {
        this.config = Objects.requireNonNull(config);
    }

    public void ensureTopic(String topicName) {
        Objects.requireNonNull(topicName);

        if (topicName.isBlank()) {
            throw new IllegalArgumentException("Kafka topic name must not be blank");
        }

        Map<String, Object> properties = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers(),
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_SECONDS * 1000,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_SECONDS * 1000);

        try (Admin admin = Admin.create(properties)) {
            createTopicIfMissing(admin, topicName);
            validatePartitionsCount(admin, topicName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException("Kafka topic preparation was interrupted: " + topicName, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to prepare Kafka topic: " + topicName, e);
        }
    }

    private void createTopicIfMissing(Admin admin, String topicName)
            throws ExecutionException, InterruptedException, TimeoutException {
        NewTopic topic = new NewTopic(topicName, config.partitionsCount(), REPLICATION_FACTOR);

        try {
            admin.createTopics(List.of(topic))
                    .all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }

    private void validatePartitionsCount(Admin admin, String topicName)
            throws ExecutionException, InterruptedException, TimeoutException {
        TopicDescription topic = admin.describeTopics(List.of(topicName))
                .allTopicNames()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(topicName);

        int actualPartitionsCount = topic.partitions().size();

        if (actualPartitionsCount != config.partitionsCount()) {
            throw new IllegalStateException("Topic '%s' has %d partitions, but configuration expects %d"
                    .formatted(topicName, actualPartitionsCount, config.partitionsCount()));
        }
    }
}