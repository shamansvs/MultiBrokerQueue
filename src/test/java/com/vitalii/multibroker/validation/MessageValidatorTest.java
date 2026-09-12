package com.vitalii.multibroker.validation;

import com.vitalii.multibroker.model.PojoMessage;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class MessageValidatorTest {
    private ValidatorFactory factory;
    private MessageValidator messageValidator;

    @BeforeEach
    void setUp() {
        factory = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory();

        messageValidator = new MessageValidator(factory.getValidator());
    }

    @AfterEach
    void tearDown() {
        factory.close();
    }

    @Test
    void shouldReturnNoViolationsForValidMessage() {
        PojoMessage message = new PojoMessage(
                "anastasia",
                "2000010100019",
                10,
                LocalDateTime.now()
        );

        Set<ConstraintViolation<PojoMessage>> violations = messageValidator.validate(message);

        assertTrue(violations.isEmpty());
    }

    @Test
    void shouldReturnViolationsForInvalidMessage() {
        PojoMessage message = new PojoMessage(
                "bbbbbb",
                "",
                9,
                LocalDateTime.now().plusDays(1)
        );

        Set<ConstraintViolation<PojoMessage>> violations =
                messageValidator.validate(message);

        Set<String> invalidFields = violations.stream()
                .map(violation -> violation.getPropertyPath().toString())
                .collect(Collectors.toSet());

        assertEquals(5, violations.size());

        assertEquals(
                Set.of("name", "eddr", "count", "createdAt"),
                invalidFields
        );
    }

}