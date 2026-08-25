package com.vitalii.multibroker.generator;

import com.vitalii.multibroker.eddr.EddrChecksum;
import com.vitalii.multibroker.model.PojoMessage;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Random;

public final class MessageGenerator {
    private final Random random;
    private static final LocalDate MIN_DATE = LocalDate.of(1900, Month.JANUARY, 1);
    private static final ZoneId UKRAINE_ZONE = ZoneId.of("Europe/Kyiv");
    private static final int MIN_NAME_LENGTH = 6;
    private static final int MAX_NAME_LENGTH = 15;
    private static final String NAME_CHARACTERS = "abcadefghiajklmnopqarstuvwxyza";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE;
    private static final int MIN_COUNT = 1;
    private static final int MAX_COUNT = 999;

    public MessageGenerator() {
        this(new Random());
    }

    public MessageGenerator(Random random) {
        this.random = Objects.requireNonNull(random);
    }

    public PojoMessage generate() {
        String name = generateRandomName();
        String eddr = generateRandomEddr();
        int count = generateRandomCount();
        LocalDateTime createdAt = LocalDateTime.now(UKRAINE_ZONE);
        return new PojoMessage(name, eddr, count, createdAt);
    }

    private String generateRandomName() {
        int nameLength = MIN_NAME_LENGTH + random.nextInt(MAX_NAME_LENGTH - MIN_NAME_LENGTH + 1);
        StringBuilder randomName = new StringBuilder(nameLength);
        for (int i = 0; i < nameLength; i++) {
            int index = random.nextInt(NAME_CHARACTERS.length());
            randomName.append(NAME_CHARACTERS.charAt(index));
        }
        return randomName.toString();
    }

    private String generateRandomEddr() {
        String randomDate = generateRandomDate();
        String randomDigits = String.format("%04d", random.nextInt(1, 10_000));
        String firstTwelveDigits = randomDate + randomDigits;
        int controlDigit = EddrChecksum.calculateControlDigit(firstTwelveDigits);
        return firstTwelveDigits + controlDigit;
    }

    private String generateRandomDate() {
        long startEpochDay = MIN_DATE.toEpochDay();
        long endEpochDay = LocalDate.now(UKRAINE_ZONE).toEpochDay();
        long randomEpochDay = random.nextLong(startEpochDay, endEpochDay + 1);
        LocalDate randomDate = LocalDate.ofEpochDay(randomEpochDay);
        return randomDate.format(DATE_FORMATTER);
    }

    private int generateRandomCount() {
        return random.nextInt(MAX_COUNT - MIN_COUNT + 1) + MIN_COUNT;
    }
}
