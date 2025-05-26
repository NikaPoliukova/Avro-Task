package com.example.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class AvroProducer {
    public static void main(String[] args) {
        String topic = "test-topic";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 5);

        try (KafkaProducer<String, Message> producer = new KafkaProducer<>(properties)) {
            Message message = Message.newBuilder()
                    .setId("124")
                    .setName("Never gonna give you up!")
                    .setAuthor("Rick Astley")
                    .setAlbum("Whenever You Need Somebody")
                    .build();

            ProducerRecord<String, Message> record = new ProducerRecord<>(topic, message.getId().toString(), message);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Successfully sent message: " + metadata);
                } else {
                    exception.printStackTrace();
                }
            });

            producer.flush();
        }
    }
}
