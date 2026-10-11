package com.vitalii.multibroker.broker.activemq;

import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.serialization.QueueMessageSerializer;
import jakarta.jms.Connection;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ActiveMqBrokerUnitTest {
    private static final String QUEUE_NAME = "test-queue";

    private ActiveMqConnectionProvider connectionProvider;
    private QueueMessageSerializer serializer;
    private Connection connection;
    private ActiveMqBroker broker;
    private ExceptionListener exceptionListener;

    @BeforeEach
    void setUp() throws JMSException {
        connectionProvider = mock(ActiveMqConnectionProvider.class);
        serializer = mock(QueueMessageSerializer.class);
        connection = mock(Connection.class);

        Session producerSession = mock(Session.class);
        MessageProducer producer = mock(MessageProducer.class);

        when(connectionProvider.createConnection()).thenReturn(connection);
        when(connection.createSession(Session.AUTO_ACKNOWLEDGE)).thenReturn(producerSession);
        when(producerSession.createProducer(null)).thenReturn(producer);
        when(connection.createSession(Session.CLIENT_ACKNOWLEDGE))
                .thenAnswer(invocation -> createConsumerSession());

        broker = new ActiveMqBroker(connectionProvider, serializer);
        ArgumentCaptor<ExceptionListener> listenerCaptor = ArgumentCaptor.forClass(ExceptionListener.class);

        verify(connection).setExceptionListener(listenerCaptor.capture());
        exceptionListener = listenerCaptor.getValue();
    }

    @AfterEach
    void tearDown() throws JMSException {
        if (broker != null) {
            broker.close();
            verify(connection).close();
            verify(connectionProvider).close();
        }
    }

    @Test
    void shouldNotifyAllHandlersAndRejectOperationsAfterConnectionFailure() throws JMSException {

        QueueMessageHandler firstHandler = mock(QueueMessageHandler.class);
        QueueMessageHandler secondHandler = mock(QueueMessageHandler.class);
        QueueMessageHandler newHandler = mock(QueueMessageHandler.class);

        broker.subscribe(QUEUE_NAME, firstHandler);
        broker.subscribe(QUEUE_NAME, secondHandler);

        JMSException connectionError = new JMSException("Connection lost");

        exceptionListener.onException(connectionError);

        verify(firstHandler).onError(connectionError);
        verify(secondHandler).onError(connectionError);

        IllegalStateException sendError = assertThrows(
                IllegalStateException.class, () -> broker.send(QUEUE_NAME, PoisonPill.STOP));

        assertSame(connectionError, sendError.getCause());

        IllegalStateException subscribeError = assertThrows(
                IllegalStateException.class, () -> broker.subscribe(QUEUE_NAME, newHandler));

        assertSame(connectionError, subscribeError.getCause());
        verifyNoInteractions(serializer);
        verifyNoInteractions(newHandler);
        verify(connection, times(2)).createSession(Session.CLIENT_ACKNOWLEDGE);
    }

    @Test
    void shouldNotifyRemainingHandlersWhenOneErrorHandlerThrows() {
        QueueMessageHandler firstHandler = mock(QueueMessageHandler.class);
        QueueMessageHandler secondHandler = mock(QueueMessageHandler.class);

        broker.subscribe(QUEUE_NAME, firstHandler);
        broker.subscribe(QUEUE_NAME, secondHandler);

        JMSException connectionError = new JMSException("Connection lost");
        RuntimeException notificationError = new IllegalStateException("Error handler failed");

        doThrow(notificationError)
                .when(firstHandler)
                .onError(connectionError);

        assertDoesNotThrow(() -> exceptionListener.onException(connectionError));
        verify(firstHandler).onError(connectionError);
        verify(secondHandler).onError(connectionError);
        verifyNoMoreInteractions(firstHandler, secondHandler);
    }

    private Session createConsumerSession() throws JMSException {
        Session session = mock(Session.class);
        Queue queue = mock(Queue.class);
        MessageConsumer consumer = mock(MessageConsumer.class);

        when(session.createQueue(QUEUE_NAME)).thenReturn(queue);
        when(session.createConsumer(queue)).thenReturn(consumer);

        return session;
    }
}