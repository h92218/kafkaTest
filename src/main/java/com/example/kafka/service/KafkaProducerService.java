package com.example.kafka.service;


import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Service
@RequiredArgsConstructor
public class KafkaProducerService {
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaAdmin kafkaAdmin;

    private final String TOPIC = "pub-test";

    public void sendMessage(String key, Object message ) {
        try{
            kafkaAdmin.createOrModifyTopics(new NewTopic(TOPIC, 1, (short) 1));
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(TOPIC, key, makeMessage("good","testMessage"));

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    RecordMetadata metadata = result.getRecordMetadata();
                } else {
                    log.error(ex.getMessage());
                }
            });
        } catch (Exception e) {
            System.out.println("produce error");
            e.printStackTrace();
        }

    }

    public HashMap<String,String> makeMessage(String status, String message){
        HashMap<String,String> result = new HashMap<String,String>();
        result.put("status", status);
        result.put("message", message);

        return result;
    }

}
