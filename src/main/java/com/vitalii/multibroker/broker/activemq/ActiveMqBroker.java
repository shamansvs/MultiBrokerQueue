package com.vitalii.multibroker.broker.activemq;

import com.vitalii.multibroker.broker.MessageBroker;
import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.serialization.QueueMessageSerializer;
import jakarta.jms.*;

import java.lang.IllegalStateException;
import java.util.HashMap;
import java.util.Map;

public class ActiveMqBroker implements MessageBroker {
    private final ActiveMqConnectionProvider connectionProvider;
    private final QueueMessageSerializer serializer;
    private final Map<String, Queue> producerQueues = new HashMap<>();
    private final Connection connection;
    private final Session producerSession;
    private final MessageProducer producer;

    public ActiveMqBroker(ActiveMqConnectionProvider connectionProvider, QueueMessageSerializer serializer) {
        this.connectionProvider = connectionProvider;
        this.serializer = serializer;

        Connection createdConnection = null;

        try {
            createdConnection = connectionProvider.createConnection();

            this.producerSession = createdConnection.createSession(Session.AUTO_ACKNOWLEDGE);
            this.producer = producerSession.createProducer(null);
            this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            this.connection = createdConnection;
        } catch (JMSException | RuntimeException e) {
            if (createdConnection != null) {
                try {
                    createdConnection.close();
                } catch (JMSException closingError) {
                    e.addSuppressed(closingError);
                }
            }

            try {
                connectionProvider.close();
            } catch (RuntimeException closingError) {
                e.addSuppressed(closingError);
            }
            throw new IllegalStateException("Failed to initialize ActiveMQ Artemis broker", e);
        }
    }

    @Override
    public synchronized void send(String queueName, QueueMessage message) {
        try {
            Queue queue = producerQueues.get(queueName);

            if (queue == null) {
                queue = producerSession.createQueue(queueName);
                producerQueues.put(queueName, queue);
            }

            BytesMessage jmsMessage = producerSession.createBytesMessage();

            jmsMessage.writeBytes(serializer.serialize(message));

            producer.send(queue, jmsMessage);
        } catch (JMSException e) {
            throw new IllegalStateException("Failed to send message to ActiveMQ Artemis", e);
        }
    }

    @Override
    public void subscribe(String queueName, QueueMessageHandler handler) {
        Session session = null;

        try {
            session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));

            consumer.setMessageListener(message -> handleMessage(message, consumer, handler));

            connection.start();
        } catch (JMSException | RuntimeException e) {
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException closingError) {
                    e.addSuppressed(closingError);
                }
            }
            throw new IllegalStateException("Failed to subscribe to queue: " + queueName, e);
        }
    }

    private void handleMessage(Message message, MessageConsumer consumer, QueueMessageHandler handler) {
        try {
            if (!(message instanceof BytesMessage bytesMessage)) {
                throw new IllegalArgumentException("Expected BytesMessage");
            }

            byte[] body = bytesMessage.getBody(byte[].class);
            QueueMessage queueMessage = serializer.deserialize(body);
            boolean shouldContinue = handler.handle(queueMessage);

            message.acknowledge();

            if (!shouldContinue) {
                consumer.close();
            }
        } catch (JMSException | RuntimeException e) {
            try {
                consumer.close();
            } catch (JMSException closingError) {
                e.addSuppressed(closingError);
            }
            handler.onError(e);
        }
    }


    @Override
    public synchronized void close() {
        try {
            connection.close();
        } catch (JMSException e) {
            throw new IllegalStateException("Failed to close ActiveMQ Artemis connection", e);
        } finally {
            connectionProvider.close();
        }
    }

}
