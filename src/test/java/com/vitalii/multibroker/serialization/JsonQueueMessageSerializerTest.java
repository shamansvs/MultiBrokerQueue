package com.vitalii.multibroker.serialization;

import com.vitalii.multibroker.model.PoisonPill;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.model.QueueMessage;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.Month;

import static org.junit.jupiter.api.Assertions.*;

class JsonQueueMessageSerializerTest {
    private final QueueMessageSerializer serializer =
            new JsonQueueMessageSerializer();

    @Test
    void shouldSerializeAndDeserializePojoMessage() {
        PojoMessage message = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.of(2025, Month.JANUARY, 15, 10, 30)
        );

        byte[] bytes = serializer.serialize(message);
        QueueMessage deserializedMessage = serializer.deserialize(bytes);

        assertEquals(message, deserializedMessage);
    }

    @Test
    void shouldSerializeAndDeserializePoisonPill() {
        byte[] bytes = serializer.serialize(PoisonPill.STOP);

        QueueMessage deserializedMessage = serializer.deserialize(bytes);

        assertSame(PoisonPill.STOP, deserializedMessage);
    }
}