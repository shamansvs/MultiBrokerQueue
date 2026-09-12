package com.vitalii.multibroker.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.QueueMessage;

import java.io.IOException;

public final class JsonQueueMessageSerializer implements QueueMessageSerializer {

    private final ObjectMapper objectMapper;

    public JsonQueueMessageSerializer() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public byte[] serialize(QueueMessage message) {
        try {
            ObjectNode root = objectMapper.createObjectNode();

            if (message instanceof PojoMessage pojoMessage) {
                root.put("type", "DATA");
                root.set("payload", objectMapper.valueToTree(pojoMessage));
            } else if (message == PoisonPill.STOP) {
                root.put("type", "STOP");
            } else {
                throw new IllegalArgumentException("Unknown queue message type");
            }

            return objectMapper.writeValueAsBytes(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize queue message", e);
        }
    }

    @Override
    public QueueMessage deserialize(byte[] bytes) {
        try {
            JsonNode root = objectMapper.readTree(bytes);
            JsonNode typeNode = root.get("type");

            if (typeNode == null) {
                throw new IllegalArgumentException("Queue message type is missing");
            }

            return switch (typeNode.asText()) {
                case "DATA" -> deserializePojoMessage(root);
                case "STOP" -> PoisonPill.STOP;
                default -> throw new IllegalArgumentException("Unknown queue message type: " + typeNode.asText());
            };
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to deserialize queue message", e);
        }
    }

    private PojoMessage deserializePojoMessage(JsonNode root) throws JsonProcessingException {
        JsonNode payload = root.get("payload");

        if (payload == null) {
            throw new IllegalArgumentException("Queue message payload is missing");
        }

        return objectMapper.treeToValue(payload, PojoMessage.class);
    }
}