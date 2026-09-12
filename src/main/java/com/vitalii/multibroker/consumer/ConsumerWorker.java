package com.vitalii.multibroker.consumer;

import com.vitalii.multibroker.broker.QueueMessageHandler;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.QueueMessage;
import com.vitalii.multibroker.processing.MessageProcessor;

import java.util.concurrent.CountDownLatch;

public final class ConsumerWorker implements QueueMessageHandler {
    private final MessageProcessor processor;
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private volatile Throwable processingError;

    private long processedMessagesCount;

    public ConsumerWorker(MessageProcessor processor) {
        this.processor = processor;
    }

    @Override
    public boolean handle(QueueMessage message) {
        if (message == PoisonPill.STOP) {
            completionLatch.countDown();
            return false;
        }

        PojoMessage pojoMessage = (PojoMessage) message;

        processor.process(pojoMessage);
        processedMessagesCount++;

        return true;
    }

    @Override
    public void onError(Throwable error) {
        processingError = error;
        completionLatch.countDown();
    }

    public void awaitCompletion() throws InterruptedException {
        completionLatch.await();

        if (processingError != null) {
            throw new IllegalStateException("Consumer processing failed", processingError);
        }
    }

    public long getProcessedMessagesCount() {
        return processedMessagesCount;
    }
}
