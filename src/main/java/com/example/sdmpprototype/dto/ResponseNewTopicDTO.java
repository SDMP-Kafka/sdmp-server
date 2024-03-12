package com.example.sdmpprototype.dto;

import com.example.sdmpprototype.domain.NewTopic;

public record ResponseNewTopicDTO(
        String message,
        NewTopic body
) {
}
