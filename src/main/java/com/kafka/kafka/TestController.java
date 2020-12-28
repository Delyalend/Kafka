package com.kafka.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class TestController {
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    @Autowired
    public TestController(KafkaProducer<String, String> kafkaProducer,
                          KafkaConsumer<String, String> consumer) {
        this.producer = kafkaProducer;
        this.consumer = consumer;
    }

    @PostMapping("/publish")
    public void messageToTopic(@RequestParam("theme") String theme,
                               @RequestParam("key") String key,
                               @RequestParam("message") String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(theme, key, message);
        try {

            producer.send(record, (recordMetadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @GetMapping("/getData")
    public void getData() {
        Map<String, Integer> custCountryMap = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                records.forEach(record -> {
                            System.out.println("topic = " + record.topic() + ", partition = " + record.partition() +
                                    ", offset = " + record.offset() + ", customer = " + record.key() + ", " +
                                    "country = " + record.value() + "\n");
                            int updatedCount = 1;
                            if (custCountryMap.containsValue(record.value())) {
                                updatedCount = custCountryMap.get(record.value()) + 1;
                            }
                            custCountryMap.put(record.value(), updatedCount);
                            try {
                                String s = objectMapper.writeValueAsString(custCountryMap);
                                System.out.println(s);
                            } catch (JsonProcessingException e) {
                                e.printStackTrace();
                            }
                        }
                );
            }
        } finally {
            consumer.close();
        }
    }

}
