package com.example.sdmpprototype.service;

import com.example.sdmpprototype.domain.ExistTopic;
import com.example.sdmpprototype.domain.Filtering;
import com.example.sdmpprototype.domain.NewTopic;
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
    public ResponseEntity<String> generateServiceFileByTopic (NewTopic newTopic) throws IOException {
        Object[] filterArray = flattenJsonValue(newTopic);


        String cmd = "java -jar ../sdmp-kafka/build/libs/sdmp-kafka-1.0-SNAPSHOT-test.jar "
                + String.join(" ", Arrays.stream(filterArray).map(Object::toString).toArray(String[]::new));

        String fileName = newTopic.inputTopic()+"_"+newTopic.outputTopic()+".service";
        String content =
                "[Unit]\n" +
                        "Description=My Kafka Streams App\n" +
                        "After=network.target\n" +
                        "\n" +
                        "[Service]\n" +
                        "User=kafka\n" +
                        "ExecStart="+ cmd +"\n"+
                        "SuccessExitStatus=143\n" +
                        "\n" +
                        "[Install]\n" +
                        "WantedBy=multi-user.target\n";
        File file = new File("../sdmp-prototype/src/main/resources/services/"
        +fileName);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(content);
            return ResponseEntity.status(HttpStatus.OK).body("Service File has been generated.");
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid topic name.");
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
