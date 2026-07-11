package org.example.controller;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

/**
 * Receives button-click events from the React frontend over HTTP
 * and publishes them to Kafka.
 *
 * Flow: frontend POST -> this controller -> Kafka topic ("funny" or "serious").
 */
@RestController
@RequestMapping("producer")
public class MultiProducerController {

    private final String TOPIC_NAME_FUNNY = "funny";      // "Enroll Now" clicks
    private final String TOPIC_NAME_SERIOUS = "serious";  // "Buy Now" clicks

    // Kafka broker address. Use "localhost:9092" when Kafka runs on this
    // machine; replace with your server's address when Kafka runs remotely.
    private final String BOOTSTRAP_SERVERS = "localhost:9092";

    private final Producer<String, String> kafkaProducer;

    public MultiProducerController() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        // Messages are plain strings, so both key and value use the String serializer.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer<>(props);
    }

    @PostMapping("/event/funny")
    public String produceFunnyEvent() {
        sendEventToKafka(TOPIC_NAME_FUNNY, "Button Clicked: Enroll Now");
        return "Funny button click event sent to Kafka";
    }

    @PostMapping("/event/serious")
    public String produceSeriousEvent() {
        sendEventToKafka(TOPIC_NAME_SERIOUS, "Button Clicked: Buy Now");
        return "Serious button click event sent to Kafka";
    }

    private void sendEventToKafka(String topic, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "buttonClick", message);

        // send() is asynchronous: the callback below runs once the broker
        // confirms (or rejects) the message, without blocking the HTTP request.
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error sending message to Kafka: " + exception.getMessage());
            } else {
                System.out.println("Message sent to Kafka, offset: " + metadata.offset());
            }
        });
    }
}
