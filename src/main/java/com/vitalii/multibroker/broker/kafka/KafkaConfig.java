package com.vitalii.multibroker.broker.kafka;

public record KafkaConfig(String bootstrapServers, String groupId, int partitionsCount) {
    public KafkaConfig {
        if (partitionsCount < 1) {
            throw new IllegalArgumentException("Kafka partitions count must be at least 1");
        }
    }
}
