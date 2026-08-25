package com.vitalii.multibroker.eddr;

public final class EddrChecksum {
    private static final int[] COEFFICIENTS = {7, 3, 1};

    private EddrChecksum() {
    }

    public static int calculateControlDigit(String firstTwelveDigits) {
        if (firstTwelveDigits == null || !firstTwelveDigits.matches("\\d{12}")) {
            throw new IllegalArgumentException("Expected exactly 12 digits");
        }

        int sum = 0;

        for (int i = 0; i < firstTwelveDigits.length(); i++) {
            int digit = firstTwelveDigits.charAt(i) - '0';
            sum += digit * COEFFICIENTS[i % COEFFICIENTS.length];
        }

        return sum % 10;
    }

    public static boolean hasValidCheckDigit(String eddr) {
        if (eddr == null || !eddr.matches("\\d{13}")) {
            return false;
        }

        String firstTwelveDigits = eddr.substring(0, 12);
        int actualControlDigit = eddr.charAt(12) - '0';

        return calculateControlDigit(firstTwelveDigits) == actualControlDigit;
    }
}
