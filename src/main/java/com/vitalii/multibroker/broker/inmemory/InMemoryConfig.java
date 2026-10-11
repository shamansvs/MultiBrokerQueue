package com.vitalii.multibroker.broker.inmemory;

public record InMemoryConfig(
        int capacity,
        int sendTimeoutSeconds
) {
    public InMemoryConfig {
        if (capacity < 1) {
            throw new IllegalArgumentException("inmemory.queue.capacity must be greater than zero");
        }

        if (sendTimeoutSeconds < 1) {
            throw new IllegalArgumentException("inmemory.send.timeout.seconds must be greater than zero");
        }
    }
}