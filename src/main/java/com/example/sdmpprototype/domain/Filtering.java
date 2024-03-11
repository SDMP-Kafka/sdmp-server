package com.example.sdmpprototype.domain;

public record Filtering(
        FilteringType type,
        String key,
        String value,
        long rangeStart,
        long rangeEnd
) {
}
