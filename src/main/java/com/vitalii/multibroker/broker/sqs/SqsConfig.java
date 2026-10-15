package com.vitalii.multibroker.broker.sqs;

public record SqsConfig(
        String region,
        String queueUrl,
        String messageGroupId
) {
}