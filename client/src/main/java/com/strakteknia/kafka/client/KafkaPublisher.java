package com.strakteknia.kafka.client;

import com.github.javafaker.Faker;
import lombok.Builder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaPublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

    private final Producer<String, String> producer;

    public KafkaPublisher(Properties properties) {
        this.producer = new KafkaProducer<>(properties);
    }

    public Future<RecordMetadata> publish(String topic, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        return publish(record);
    }

    public Future<RecordMetadata> publish(ProducerRecord<String, String> record) {
        return producer.send(record);
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("linger.ms", 1);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaPublisher kafkaPublisher = new KafkaPublisher(properties);

        final Faker faker = new Faker();

        int n = 10;
        AtomicInteger count = new AtomicInteger();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(() -> {
            String message = String.format("Hello %s!", faker.name().firstName());
            kafkaPublisher.publish("topic_hello", message);
            kafkaPublisher.producer.flush();

            log.info("Published {} messages", count.incrementAndGet());
        }, 0 , 5, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaPublisher.producer.close()));
    }
}
