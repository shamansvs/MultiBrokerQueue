package com.vitalii.multibroker.broker.sqs;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.Objects;

public final class SqsClientProvider {
    private final SqsConfig config;

    public SqsClientProvider(SqsConfig config) {
        this.config = Objects.requireNonNull(config, "SQS config must not be null");
    }

    public SqsClient createClient() {
        return SqsClient.builder().region(Region.of(config.region())).build();
    }
}