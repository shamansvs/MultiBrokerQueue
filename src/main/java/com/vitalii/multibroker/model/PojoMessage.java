package com.vitalii.multibroker.model;

import com.vitalii.multibroker.validation.ValidEddr;
import jakarta.validation.constraints.*;

import java.time.LocalDateTime;

public record PojoMessage(
        @NotBlank(message = "name must not be blank")
        @Size(min = 7, message = "name length must be at least 7")
        @Pattern(regexp = "(?i)[^a]*a.*", message = "name must contain letter 'a'")
        String name,

        @NotBlank(message = "eddr must not be blank")
        @ValidEddr
        String eddr,

        @Min(value = 10, message = "count must be >= 10")
        int count,

        @NotNull(message = "createdAt must not be null")
        @PastOrPresent(message = "createdAt must be in the past or present")
        LocalDateTime createdAt
) {
}
