package com.vitalii.multibroker.validation;

import com.vitalii.multibroker.model.PojoMessage;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

import java.util.Objects;
import java.util.Set;

public class MessageValidator {
    private final Validator validator;

    public MessageValidator(Validator validator) {
        this.validator = Objects.requireNonNull(validator);
    }

    public Set<ConstraintViolation<PojoMessage>> validate(PojoMessage message) {
        return validator.validate(message);
    }

    public boolean isValid(PojoMessage message) {
        return validate(message).isEmpty();
    }
}
