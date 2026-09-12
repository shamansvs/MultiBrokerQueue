package com.vitalii.multibroker.validation;

import com.vitalii.multibroker.eddr.EddrChecksum;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EddrValidatorTest {

    private final EddrValidator validator = new EddrValidator();

    @Test
    void shouldReturnTrueForValidEddr() {
        boolean actual = validator.isValid("2000010100019", null);

        assertTrue(actual);
    }

    @Test
    void shouldReturnFalseForIncorrectEddr() {
        String futureEddr = "209901010001" + EddrChecksum.calculateControlDigit("209901010001");

        assertAll(
                () -> assertFalse(validator.isValid("200001010001", null)),
                () -> assertFalse(validator.isValid("20000101000A9", null)),
                () -> assertFalse(validator.isValid("2000023000010", null)),
                () -> assertFalse(validator.isValid("2000010100015", null)),
                () -> assertFalse(validator.isValid(futureEddr, null))
        );
    }

    @Test
    void shouldReturnTrueForNullOrBlankValue() {
        assertAll(
                () -> assertTrue(validator.isValid(null, null)),
                () -> assertTrue(validator.isValid("", null)),
                () -> assertTrue(validator.isValid("   ", null))
        );
    }
}