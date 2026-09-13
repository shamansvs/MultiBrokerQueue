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

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaBroker implements MessageBroker {
    private static final int PARTITION = 0;

    private final QueueMessageSerializer serializer;
    private final KafkaProducer<String, byte[]> producer;
    private final AtomicReference<Exception> sendingError = new AtomicReference<>();
    private final KafkaConfig config;
    private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();

    private KafkaSubscription subscription;
    private boolean closed;

    public KafkaBroker(KafkaConfig config, QueueMessageSerializer serializer) {
        this.config = Objects.requireNonNull(config);
        this.serializer = Objects.requireNonNull(serializer);
        this.producer = createProducer(config);
    }

    @Override
    public void send(String queueName, QueueMessage message) {
        checkSendingError();

        boolean isStop = message == PoisonPill.STOP;

        if (isStop) {
            awaitPendingMessages();
        }

        byte[] body = serializer.serialize(message);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                queueName,
                PARTITION,
                null,
                body
        );

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                sendingError.compareAndSet(null, exception);
            }
        });

        if (isStop) {
            awaitPendingMessages();
        }
    }

    @Override
    public void subscribe(
            String queueName,
            QueueMessageHandler handler
    ) {
        if (closed) {
            throw new IllegalStateException("Kafka broker is closed");
        }

        if (subscription != null) {
            throw new IllegalStateException("Only one Kafka consumer is supported for now");
        }

        subscription = new KafkaSubscription(
                config,
                queueName,
                serializer,
                handler
        );

        consumerExecutor.execute(subscription);
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

    private void checkSendingError() {
        Exception error = sendingError.get();

        if (error != null) {
            throw new IllegalStateException(
                    "Failed to send message to Kafka",
                    error
            );
        }
    }

    private void awaitPendingMessages() {
        producer.flush();
        checkSendingError();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        closed = true;

        try (producer; consumerExecutor) {
            if (subscription != null) {
                subscription.requestStop();
            }
        }

        checkSendingError();
    }
}
