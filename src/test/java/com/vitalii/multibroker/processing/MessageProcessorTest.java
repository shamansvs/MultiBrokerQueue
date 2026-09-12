package com.vitalii.multibroker.processing;

import com.vitalii.multibroker.csv.InvalidCsvWriter;
import com.vitalii.multibroker.csv.ValidCsvWriter;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.validation.MessageValidator;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
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
        ValidCsvWriter validWriter = mock(ValidCsvWriter.class);
        InvalidCsvWriter invalidWriter = mock(InvalidCsvWriter.class);

        try (ValidatorFactory factory = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()) {

            MessageValidator validator =
                    new MessageValidator(factory.getValidator());

            MessageProcessor processor =
                    new MessageProcessor(validator, validWriter, invalidWriter);

            PojoMessage invalidMessage = new PojoMessage(
                    "bbbbbb",
                    "",
                    9,
                    LocalDateTime.now().plusDays(1)
            );

            processor.process(invalidMessage);

            verify(invalidWriter).write(
                    eq(invalidMessage),
                    anySet()
            );
            verifyNoInteractions(validWriter);
        }
    }

}