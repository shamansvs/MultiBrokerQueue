package com.vitalii.multibroker.eddr;

import org.junit.jupiter.api.Test;

import static com.vitalii.multibroker.eddr.EddrChecksum.calculateControlDigit;
import static org.junit.jupiter.api.Assertions.*;

class EddrChecksumTest {
    @Test
    void shouldCalculateControlDigit() {
        String firstTwelveDigits = "200001010001";

        int controlDigit = calculateControlDigit(firstTwelveDigits);

        assertEquals(9, controlDigit);
    }

    @Test
    void shouldReturnTrueForValidEddr() {
        String validEddr = "2000010100019";

        boolean actual = EddrChecksum.hasValidCheckDigit(validEddr);

        assertTrue(actual);
    }

    @Test
    void shouldReturnFalseForInvalidEddr() {
        assertAll(
                () -> assertFalse(
                        EddrChecksum.hasValidCheckDigit("2000010100014")
                ),
                () -> assertFalse(
                        EddrChecksum.hasValidCheckDigit("200001010001")
                ),
                () -> assertFalse(
                        EddrChecksum.hasValidCheckDigit("200001010001A")
                ),
                () -> assertFalse(
                        EddrChecksum.hasValidCheckDigit(null)
                )
        );
    }

    @Test
    void shouldThrowExceptionForInvalidInput() {
        assertAll(
                () -> assertThrows(
                        IllegalArgumentException.class,
                        () -> calculateControlDigit(null)
                ),
                () -> assertThrows(
                        IllegalArgumentException.class,
                        () -> calculateControlDigit("123456")
                ),
                () -> assertThrows(
                        IllegalArgumentException.class,
                        () -> calculateControlDigit("1234567890123")
                ),
                () -> assertThrows(
                        IllegalArgumentException.class,
                        () -> calculateControlDigit("12345678901A")
                )
        );
    }

}