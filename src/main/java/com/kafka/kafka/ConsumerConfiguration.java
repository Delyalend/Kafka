package com.kafka.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Properties;

@Configuration
public class ConsumerConfiguration {

    @Bean
    public KafkaConsumer<String,String> kafkaConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("group.id","CountryCounter");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("customerCountries"));
        return consumer;

    }

}
