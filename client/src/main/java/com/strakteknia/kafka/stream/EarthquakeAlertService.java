package com.strakteknia.kafka.stream;

import lombok.Builder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Builder
public class EarthquakeAlertService {
    private static final Logger log = LoggerFactory.getLogger(EarthquakeAlertService.class);

    private final Consumer<String, String> consumer;

    public void subscribe(List<String> topics) {
        consumer.subscribe(topics);
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "test_subscriber");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        EarthquakeAlertService kafkaSubscriber = EarthquakeAlertService.builder()
                .consumer(consumer)
                .build();

        List<String> topics = List.of("earthquake_data");
        log.info("Subscribing to: {}", topics);

        kafkaSubscriber.subscribe(topics);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaSubscriber.consumer.unsubscribe()));

        while (true) {
            ConsumerRecords<String, String> records = kafkaSubscriber.consumer.poll(Duration.ofSeconds(5));
            for(ConsumerRecord<String, String> r : records) {
                log.info("Topic: {}   Value: {}", r.topic(), r.value());
            }
        }

    }
}
