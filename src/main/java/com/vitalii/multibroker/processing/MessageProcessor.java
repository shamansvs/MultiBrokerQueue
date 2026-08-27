package com.vitalii.multibroker.processing;

import com.vitalii.multibroker.csv.InvalidCsvWriter;
import com.vitalii.multibroker.csv.ValidCsvWriter;
import com.vitalii.multibroker.model.PojoMessage;
import com.vitalii.multibroker.validation.MessageValidator;
import jakarta.validation.ConstraintViolation;

import java.util.Set;

public final class MessageProcessor {
    private final MessageValidator validator;
    private final ValidCsvWriter validWriter;
    private final InvalidCsvWriter invalidWriter;

    public MessageProcessor(MessageValidator validator, ValidCsvWriter validWriter, InvalidCsvWriter invalidWriter) {
        this.validator = validator;
        this.validWriter = validWriter;
        this.invalidWriter = invalidWriter;
    }

    public void process(PojoMessage message) {
        Set<ConstraintViolation<PojoMessage>> violations =
                validator.validate(message);

        if (violations.isEmpty()) {
            validWriter.write(message);
        } else {
            invalidWriter.write(message, violations);
        }
    }
}
