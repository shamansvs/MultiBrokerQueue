package com.vitalii.multibroker.processing;

import com.vitalii.multibroker.csv.InvalidCsvWriter;
import com.vitalii.multibroker.csv.ValidCsvWriter;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.validation.MessageValidator;
import jakarta.validation.ConstraintViolation;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Set;

import static org.mockito.Mockito.*;

class MessageProcessorTest {
    private final PojoMessage message = new PojoMessage(
            "anastasia",
            "2000010100019",
            10,
            LocalDateTime.now()
    );

    @Test
    void shouldWriteValidMessageToValidCsv() {
        MessageValidator validator = mock(MessageValidator.class);
        ValidCsvWriter validWriter = mock(ValidCsvWriter.class);
        InvalidCsvWriter invalidWriter = mock(InvalidCsvWriter.class);

        MessageProcessor processor =
                new MessageProcessor(validator, validWriter, invalidWriter);

        when(validator.validate(message)).thenReturn(Set.of());

        processor.process(message);

        verify(validWriter).write(message);
        verifyNoInteractions(invalidWriter);
    }

    @Test
    void shouldWriteInvalidMessageToInvalidCsv() {
        MessageValidator validator = mock(MessageValidator.class);
        ValidCsvWriter validWriter = mock(ValidCsvWriter.class);
        InvalidCsvWriter invalidWriter = mock(InvalidCsvWriter.class);

        MessageProcessor processor =
                new MessageProcessor(validator, validWriter, invalidWriter);

        @SuppressWarnings("unchecked")
        ConstraintViolation<PojoMessage> violation =
                mock(ConstraintViolation.class);

        Set<ConstraintViolation<PojoMessage>> violations = Set.of(violation);

        when(validator.validate(message)).thenReturn(violations);

        processor.process(message);

        verify(invalidWriter).write(message, violations);
        verifyNoInteractions(validWriter);
    }

}