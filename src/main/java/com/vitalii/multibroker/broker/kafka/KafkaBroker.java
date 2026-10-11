package com.vitalii.multibroker.broker.kafka;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.serialization.QueueMessageSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public final class KafkaBroker implements MessageBroker {
    private final KafkaConfig config;
    private final QueueMessageSerializer serializer;
    private final KafkaProducer<String, byte[]> producer;
    private final ExecutorService consumerExecutor;

    private final List<KafkaSubscription> subscriptions = new ArrayList<>();
    private final AtomicReference<Exception> sendingError = new AtomicReference<>();

    private String validatedQueueName;
    private int nextMessagePartition;
    private int nextStopPartition;
    private boolean closed;

    public KafkaBroker(KafkaConfig config, QueueMessageSerializer serializer) {
        this.config = Objects.requireNonNull(config);
        this.serializer = Objects.requireNonNull(serializer);
        this.producer = createProducer(config);
        this.consumerExecutor = Executors.newFixedThreadPool(config.partitionsCount());
    }

    private KafkaProducer<String, byte[]> createProducer(KafkaConfig config) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new KafkaProducer<>(properties);
    }

    @Override
    public synchronized void send(String queueName, QueueMessage message) {
        ensureOpen();
        Objects.requireNonNull(message);
        checkSendingError();
        ensureTopic(queueName);

        if (message == PoisonPill.STOP) {
            sendStop(queueName);
            return;
        }

        if (nextStopPartition > 0) {
            throw new IllegalStateException("Cannot send data after poison pills");
        }

        sendToPartition(queueName, nextMessagePartition, message);

        nextMessagePartition = (nextMessagePartition + 1) % config.partitionsCount();
    }

    private void sendStop(String queueName) {
        if (nextStopPartition >= config.partitionsCount()) {
            throw new IllegalStateException("Poison pills have already been sent to all partitions");
        }

        awaitPendingMessages();

        sendToPartition(queueName, nextStopPartition, PoisonPill.STOP);

        nextStopPartition++;

        awaitPendingMessages();
    }

    private void sendToPartition(String queueName, int partition, QueueMessage message) {
        byte[] body = serializer.serialize(message);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(queueName, partition, null, body);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                sendingError.compareAndSet(null, exception);
            }
        });
    }

    @Override
    public synchronized void subscribe(String queueName, QueueMessageHandler handler) {
        ensureOpen();
        Objects.requireNonNull(handler);
        ensureTopic(queueName);

        if (subscriptions.size() >= config.partitionsCount()) {
            throw new IllegalStateException("All Kafka partitions already have consumers");
        }

        int partition = subscriptions.size();

        KafkaSubscription subscription = new KafkaSubscription(config, queueName, partition, serializer, handler);

        subscriptions.add(subscription);
        consumerExecutor.execute(subscription);
    }

    private void ensureTopic(String queueName) {
        Objects.requireNonNull(queueName);

        if (validatedQueueName != null) {
            if (!validatedQueueName.equals(queueName)) {
                throw new IllegalArgumentException("This KafkaBroker supports only one topic");
            }
            return;
        }

        int actualPartitionsCount = producer.partitionsFor(queueName).size();

        if (actualPartitionsCount != config.partitionsCount()) {
            throw new IllegalStateException("Topic '%s' has %d partitions, but configuration expects %d"
                    .formatted(queueName, actualPartitionsCount, config.partitionsCount()));
        }

        validatedQueueName = queueName;
    }

    private void awaitPendingMessages() {
        producer.flush();
        checkSendingError();
    }

    private void checkSendingError() {
        Exception error = sendingError.get();

        if (error != null) {
            throw new IllegalStateException("Failed to send message to Kafka", error);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Kafka broker is closed");
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        closed = true;

        try (producer; consumerExecutor) {
            for (KafkaSubscription subscription : subscriptions) {
                subscription.requestStop();
            }
        }

        checkSendingError();
    }
}