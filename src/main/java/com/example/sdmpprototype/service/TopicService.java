package com.example.sdmpprototype.service;

import com.example.sdmpprototype.domain.ExistTopic;
import com.example.sdmpprototype.domain.Filtering;
import com.example.sdmpprototype.domain.NewTopic;
import com.example.sdmpprototype.dto.ResponseNewTopicDTO;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class TopicService {
    public ResponseEntity<ResponseNewTopicDTO> generateServiceFileByTopic (NewTopic newTopic) throws IOException {
        Object[] filterArray = flattenJsonValue(newTopic);


        String cmd = "java -jar ../sdmp-kafka/build/libs/sdmp-kafka-1.0-SNAPSHOT-test.jar "
                + String.join(" ", Arrays.stream(filterArray).map(Object::toString).toArray(String[]::new));
        ProcessBuilder processBuilder = new ProcessBuilder();
        System.out.println(cmd);
        processBuilder.command("bash","-c",cmd);
        try {
            processBuilder.start();
            return ResponseEntity.status(HttpStatus.OK)
                    .body(new ResponseNewTopicDTO("Execute Java Successful",newTopic));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ResponseNewTopicDTO("Error Java executing",null));
        }
    }

        private Object[] flattenJsonValue(NewTopic newTopic){
        List<Object> values = new ArrayList<>();
        values.add(newTopic.inputTopic());
        values.add(newTopic.outputTopic());
        for (Filtering filtering : newTopic.filtering()) {
            String type = String.valueOf(filtering.type());
            values.add(filtering.type());
            values.add(filtering.key());
            if (Objects.equals(type, "STRING") || Objects.equals(type, "NUMBER")){
                values.add(filtering.value());
            }else{
                values.add(String.valueOf(filtering.rangeStart()));
                values.add(String.valueOf(filtering.rangeEnd()));
            }
        }
        return values.toArray();
    }


    public ResponseEntity<List<ExistTopic>> getAllTopics() {
        String bootstrapServers = "localhost:9092";
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            ListTopicsResult topicsResult = adminClient.listTopics(new ListTopicsOptions().listInternal(true));
            Set<String> topicNames = topicsResult.names().get();
            List<ExistTopic> existTopics = new ArrayList<ExistTopic>();
            for (String topicName : topicNames) {
                ExistTopic existTopic = new ExistTopic(topicName);
                existTopics.add(existTopic);
            }
            return ResponseEntity.status(HttpStatus.OK).body(existTopics);
        } catch (InterruptedException | ExecutionException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ArrayList<>());
        }
    }
}
