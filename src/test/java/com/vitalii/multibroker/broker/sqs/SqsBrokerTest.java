package com.vitalii.multibroker.broker.sqs;

import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.serialization.QueueMessageSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqsBrokerTest {
    private static final String QUEUE_NAME = "test-messages.fifo";
    private static final String QUEUE_URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/" + QUEUE_NAME;

    @Test
    void shouldProcessMessageAndDeleteStopBeforeNotifyingHandler() throws InterruptedException {
        SqsClient client = mock(SqsClient.class);
        QueueMessageSerializer serializer = mock(QueueMessageSerializer.class);
        QueueMessageHandler handler = mock(QueueMessageHandler.class);
        SqsConfig config = new SqsConfig("eu-central-1", QUEUE_URL, "test-group");
        PojoMessage pojoMessage = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.of(2026, 1, 15, 10, 30)
        );

        Message dataMessage = Message.builder().body("data-body").receiptHandle("data-receipt").build();
        Message stopMessage = Message.builder().body("stop-body").receiptHandle("stop-receipt").build();

        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(
                ReceiveMessageResponse.builder().messages(dataMessage).build(),
                ReceiveMessageResponse.builder().messages(stopMessage).build());

        when(serializer.deserialize("data-body".getBytes(StandardCharsets.UTF_8))).thenReturn(pojoMessage);
        when(serializer.deserialize("stop-body".getBytes(StandardCharsets.UTF_8))).thenReturn(PoisonPill.STOP);
        when(handler.handle(pojoMessage)).thenReturn(true);

        CountDownLatch completion = new CountDownLatch(1);

        when(handler.handle(PoisonPill.STOP)).thenAnswer(invocation -> {
            completion.countDown();
            return false;
        });

        try (SqsBroker broker = new SqsBroker(client, config, serializer)) {
            broker.subscribe(QUEUE_NAME, handler);
            assertTrue(completion.await(2, TimeUnit.SECONDS), "Consumer did not receive STOP");
        }

        DeleteMessageRequest deleteData = DeleteMessageRequest.builder()
                .queueUrl(QUEUE_URL).receiptHandle("data-receipt").build();

        DeleteMessageRequest deleteStop = DeleteMessageRequest.builder()
                .queueUrl(QUEUE_URL).receiptHandle("stop-receipt").build();

        InOrder order = inOrder(handler, client);

        order.verify(handler).handle(pojoMessage);
        order.verify(client).deleteMessage(deleteData);
        order.verify(client).deleteMessage(deleteStop);
        order.verify(handler).handle(PoisonPill.STOP);
        order.verify(client).close();

        verify(client, times(2)).receiveMessage(any(ReceiveMessageRequest.class));
        verify(handler, never()).onError(any());
    }

    @Test
    void shouldReportProcessingFailureWithoutDeletingMessage() throws InterruptedException {
        SqsClient client = mock(SqsClient.class);
        QueueMessageSerializer serializer = mock(QueueMessageSerializer.class);
        QueueMessageHandler handler = mock(QueueMessageHandler.class);
        SqsConfig config = new SqsConfig("eu-central-1", QUEUE_URL, "test-group");
        PojoMessage pojoMessage = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.of(2026, 1, 15, 10, 30)
        );

        Message message = Message.builder().body("data-body").receiptHandle("data-receipt").build();

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(message).build());
        when(serializer.deserialize("data-body".getBytes(StandardCharsets.UTF_8))).thenReturn(pojoMessage);

        IllegalStateException expectedError = new IllegalStateException("CSV writing failed");

        when(handler.handle(pojoMessage)).thenThrow(expectedError);

        CountDownLatch errorReported = new CountDownLatch(1);

        doAnswer(invocation -> {
            errorReported.countDown();
            return null;
        }).when(handler).onError(expectedError);

        try (SqsBroker broker = new SqsBroker(client, config, serializer)) {
            broker.subscribe(QUEUE_NAME, handler);

            assertTrue(errorReported.await(2, TimeUnit.SECONDS),
                    "Consumer did not report the processing error");
        }

        verify(handler).handle(pojoMessage);
        verify(handler).onError(expectedError);
        verify(client, never()).deleteMessage(any(DeleteMessageRequest.class));
        verify(client, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
        verify(client).close();
    }

    @Test
    void shouldSendMessagesWithDistinctDeduplicationIds() {
        SqsClient client = mock(SqsClient.class);
        QueueMessageSerializer serializer = mock(QueueMessageSerializer.class);
        SqsConfig config = new SqsConfig("eu-central-1", QUEUE_URL, "test-group"
        );

        when(serializer.serialize(PoisonPill.STOP)).thenReturn("stop-body".getBytes(StandardCharsets.UTF_8));

        try (SqsBroker broker = new SqsBroker(client, config, serializer)) {
            broker.send(QUEUE_NAME, PoisonPill.STOP);
            broker.send(QUEUE_NAME, PoisonPill.STOP);
        }

        ArgumentCaptor<SendMessageRequest> requestCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);

        verify(client, times(2)).sendMessage(requestCaptor.capture());

        var requests = requestCaptor.getAllValues();

        for (SendMessageRequest request : requests) {
            assertEquals(QUEUE_URL, request.queueUrl());
            assertEquals("stop-body", request.messageBody());
            assertEquals("test-group", request.messageGroupId());
            assertFalse(request.messageDeduplicationId().isBlank());
        }

        assertNotEquals(
                requests.get(0).messageDeduplicationId(),
                requests.get(1).messageDeduplicationId()
        );

        verify(client).close();
    }
}