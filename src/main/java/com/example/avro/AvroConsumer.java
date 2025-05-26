package com.example.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("specific.avro.reader", "true");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("test-topic"));

            while (true) {
                ConsumerRecords<String, Message> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, Message> record : records) {
                    System.out.println("Received message: " + record.value());
                }
            }
        }
    }
}

