package com.example.sdmpprototype.domain;

import java.util.List;

public record NewTopic(
        String inputTopic,
        String outputTopic,
        List<Filtering> filtering
) {
}

