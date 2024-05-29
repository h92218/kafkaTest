package com.example.kafka.service;


import static org.springframework.http.MediaType.valueOf;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerAwareMessageListener;
import org.springframework.stereotype.Component;


import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;


@Component
@Log4j2
@RequiredArgsConstructor
public class KafkaConsumerService implements ConsumerAwareMessageListener<String, Object>{

    private final KafkaProducerService kafkaProducerService;
    private static final String TOPIC = "pub-test";

    // 백그라운드로 poll을 반복적으로 수행함
    @KafkaListener(topics = TOPIC, groupId = "group1")
    public void onMessage(ConsumerRecord<String, Object> record, Consumer<?, ?> consumer) {

        try {
            log.info(record.toString());
            // 오프셋 커밋 수행: 동기(커밋 되었는지 확인함), 비동기(커밋 되었는지 확인하지 않음)
            consumer.commitSync(); //commitAsync()


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
