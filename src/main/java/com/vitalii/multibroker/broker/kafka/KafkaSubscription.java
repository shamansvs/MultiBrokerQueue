package com.vitalii.multibroker.broker.kafka;

import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.serialization.QueueMessageSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public final class KafkaSubscription implements Runnable {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final KafkaConsumer<String, byte[]> consumer;
    private final QueueMessageSerializer serializer;
    private final QueueMessageHandler handler;
    private final TopicPartition topicPartition;

    private volatile boolean stopRequested;

    public KafkaSubscription(KafkaConfig config, String queueName, int partition,
                             QueueMessageSerializer serializer, QueueMessageHandler handler) {
        Objects.requireNonNull(config);

        if (partition < 0 || partition >= config.partitionsCount()) {
            throw new IllegalArgumentException("Invalid Kafka partition: " + partition);
        }

        this.topicPartition = new TopicPartition(Objects.requireNonNull(queueName), partition);
        this.serializer = Objects.requireNonNull(serializer);
        this.handler = Objects.requireNonNull(handler);
        this.consumer = createConsumer(config);
    }

    private KafkaConsumer<String, byte[]> createConsumer(KafkaConfig config) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(properties);
    }

    @Override
    public void run() {
        try {
            boolean receivedStop;

            try (consumer) {
                consumer.assign(List.of(topicPartition));
                receivedStop = readMessages();
            }

            if (receivedStop) {
                handler.handle(PoisonPill.STOP);
            }
        } catch (WakeupException e) {
            if (!stopRequested) {
                handler.onError(e);
            }
        } catch (RuntimeException e) {
            handler.onError(e);
        }
    }

    private boolean readMessages() {
        while (!stopRequested) {
            ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);

            for (ConsumerRecord<String, byte[]> record : records) {
                QueueMessage message = serializer.deserialize(record.value());

                if (message == PoisonPill.STOP) {
                    commitThrough(record);
                    return true;
                }

                boolean shouldContinue = handler.handle(message);

                if (!shouldContinue) {
                    commitThrough(record);
                    return false;
                }
            }

            if (!records.isEmpty()) {
                consumer.commitSync();
            }
        }

        return false;
    }

    private void commitThrough(ConsumerRecord<String, byte[]> record) {
        OffsetAndMetadata nextOffset =
                new OffsetAndMetadata(record.offset() + 1);

        consumer.commitSync(Map.of(topicPartition, nextOffset));
    }

    public void requestStop() {
        stopRequested = true;
        consumer.wakeup();
    }
}