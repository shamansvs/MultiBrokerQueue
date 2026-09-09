package com.vitalii.multibroker.eddr;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EddrChecksumTest {
    @Test
    void shouldCalculateControlDigit() {
        String firstTwelveDigits = "200001010001";

        int controlDigit = EddrChecksum.calculateControlDigit(firstTwelveDigits);

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
}