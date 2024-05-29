package com.example.kafka.config;

import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.time.Duration;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Configuration
@EnableKafka
@RequiredArgsConstructor
public class KafkaConfig {

    @Value("${spring.kafka.properties.bootstrap-servers}")
    private String kafkaBootstrapServers;

    private Map<String, Object> getProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName=\"hyunsun\";");
        props.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        return props;
    }


    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> props = getProps();
        return new KafkaAdmin(props);
    }


    //주문 완료 시 consume
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = getProps();
        //props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        props.put("enable.auto.commit", false); // 주기적으로 offset을 commit (컨슈머 서비스가 정상적으로 처리되었을 때 커밋)
        props.put("auto.offset.reset", "earliest"); // 가장 초기의 offset 값으로 리밸런스

        //The class '' is not in the trusted packages 오류 시 false로 해주면 됨
        //false 옵션 : 메세지 헤더의 패키지명이 포함되어있으므로 헤더를 검사하지 않음 
        return new DefaultKafkaConsumerFactory<>(props,new StringDeserializer(), new JsonDeserializer<>(Object.class,false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        //레코드 단위로 프로세싱 이후 커밋
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        //인증 오류 시 재시도 간격 설정
        factory.getContainerProperties().setAuthExceptionRetryInterval(Duration.ofSeconds(5));
        return factory;
    }

    //주문완료 메세지 처리 후 produce
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = getProps();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all"); // min.insync.replicas 만큼 저장이 되어야 함.
        // acks = 0: 저장이 되었는지 확인하지 않음(유실 가능성 있음)
        // acks = 1: 리더의 파티션에 저장 되었는지 확인함(다른 파티션에 복제되지 않을 수 있음)
        // acks = all: 설정한 파티션의 수 만큼 저장 되었는지 확인함
        props.put("replication.factor", 1); // 토픽을 이루는 파티션의 복제 수(브로커 수)
        props.put("min.insync.replicas", 2); // 필수 저장되는 최소 복제본 수
        props.put("enable.idempotence", true); // 멱등성 보장(재발행 해도 데이터 중복 저장 X)
        props.put("retries", 3); // 재시도 횟수
        props.put("num.partitions", 1);  // 토픽별 파티션 수
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }


}
