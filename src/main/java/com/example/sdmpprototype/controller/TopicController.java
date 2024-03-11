package com.example.sdmpprototype.controller;

import com.example.sdmpprototype.domain.ExistTopic;
import com.example.sdmpprototype.domain.NewTopic;
import com.example.sdmpprototype.service.TopicService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("/v1/topic")
public class TopicController {
    private TopicService topicService;

    public TopicController(TopicService topicService) {
        this.topicService = topicService;
    }

    @PostMapping("/newtopic")
    public ResponseEntity<String> generateServiceFileByTopic(@RequestBody NewTopic newTopic) throws IOException {
        return this.topicService.generateServiceFileByTopic(newTopic);
    }

    @GetMapping("")
    public ResponseEntity<List<ExistTopic>> getAllTopics(){
        return this.topicService.getAllTopics();
    }
}
