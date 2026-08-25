package com.vitalii.multibroker.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.*;

@Target({ElementType.FIELD, ElementType.RECORD_COMPONENT, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = EddrValidator.class)
@Documented
public @interface ValidEddr {
    String message() default "eddr must be a valid EDDR number";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}