package com.vitalii.multibroker.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;

public class EddrValidator implements ConstraintValidator<ValidEddr, String> {
    private static final LocalDate MIN_DATE = LocalDate.of(1900, Month.JANUARY, 1);
    private static final int EDDR_LENGTH = 13;
    private static final int[] COEFFICIENTS = {7, 3, 1};
    private static final ZoneId UKRAINE_ZONE = ZoneId.of("Europe/Kyiv");
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("uuuuMMdd")
            .withResolverStyle(ResolverStyle.STRICT);

    @Override
    public boolean isValid(String eddr, ConstraintValidatorContext context) {
        if (eddr == null || eddr.isBlank()) {
            return true;
        }
        eddr = eddr.replace("-", "");

        return isValidLength(eddr) && isValidDigits(eddr) && isValidDatePart(eddr) && isValidCheckDigit(eddr);

    }

    private boolean isValidLength(String eddr) {
        return eddr.length() == EDDR_LENGTH;
    }

    private boolean isValidDigits(String eddr) {
        for (char c : eddr.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }

    private boolean isValidDatePart(String eddr) {
        String datePart = eddr.substring(0, 8);

        try {
            LocalDate date = LocalDate.parse(datePart, FORMATTER);
            LocalDate maxDate = LocalDate.now(UKRAINE_ZONE);

            return !date.isBefore(MIN_DATE) && !date.isAfter(maxDate);
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private boolean isValidCheckDigit(String eddr) {
        int sum = 0;
        for (int i = 0; i < eddr.length() - 1; i++) {
            int digit = Character.getNumericValue(eddr.charAt(i));
            sum += digit * COEFFICIENTS[i % COEFFICIENTS.length];
        }
        int lastDigit = Character.getNumericValue(eddr.charAt(eddr.length() - 1));
        return sum % 10 == lastDigit;
    }
}
