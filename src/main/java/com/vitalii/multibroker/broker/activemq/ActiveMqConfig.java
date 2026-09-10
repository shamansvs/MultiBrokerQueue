package com.vitalii.multibroker.broker.activemq;

public record ActiveMqConfig(
        String host,
        int port,
        String username,
        String password) {
}
