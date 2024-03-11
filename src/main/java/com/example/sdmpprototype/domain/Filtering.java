package com.example.sdmpprototype.domain;

public record Filtering(
        String key,
        FilteringType type,
        String value,
        long rangeStart,
        long rangeEnd
) {
}
