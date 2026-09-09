package com.vitalii.multibroker.generator;

import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.validation.EddrValidator;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class MessageGeneratorTest {
    private static final ZoneId UKRAINE_ZONE = ZoneId.of("Europe/Kyiv");

    @Test
    void shouldGenerateMessagesWithExpectedValues() {
        MessageGenerator generator = new MessageGenerator(new Random(42));
        EddrValidator eddrValidator = new EddrValidator();

        List<PojoMessage> messages = IntStream.range(0, 100)
                .mapToObj(ignored -> generator.generate())
                .toList();

        LocalDateTime currentTime = LocalDateTime.now(UKRAINE_ZONE);

        assertEquals(100, messages.size());

        assertTrue(messages.stream().allMatch(message ->
                message.name().length() >= 6
                        && message.name().length() <= 15
                        && message.count() >= 1
                        && message.count() <= 999
                        && eddrValidator.isValid(message.eddr(), null)
                        && !message.createdAt().isAfter(currentTime)
        ));
    }
}