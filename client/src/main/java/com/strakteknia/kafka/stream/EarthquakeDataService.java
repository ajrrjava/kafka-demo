package com.strakteknia.kafka.stream;

import lombok.Builder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.*;

@Builder
public class EarthquakeDataService {
    private static final Logger log = LoggerFactory.getLogger(EarthquakeDataService.class);

    private final Producer<String, String> producer;
    private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    private final HttpClient httpClient;

    private void start() {
        String url = String.format("https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=%s",
                LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
        URI uri = URI.create(url);
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .build();
        final String topic = "earthquake_data";
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
        service.scheduleAtFixedRate(() -> {
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                log.info("Received {}", response.statusCode());
                queue.offer(response.body());
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 0 , 5, TimeUnit.SECONDS);

        while(true) {
            try {
                final String message = queue.take();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
                producer.send(record);
                producer.flush();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String process(HttpResponse.BodyHandler b) {
        return null;
    }

    public Future<RecordMetadata> publish(String topic, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        return publish(record);
    }

    public Future<RecordMetadata> publish(ProducerRecord<String, String> record) {
        return producer.send(record);
    }

    private void close() {
        producer.close();
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("linger.ms", 1);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        HttpClient httpClient = HttpClient.newBuilder().build();

        EarthquakeDataService service = EarthquakeDataService.builder()
                .producer(producer)
                .httpClient(httpClient)
                .build();
        service.start();

        Runtime.getRuntime().addShutdownHook(new Thread(service::close));
    }
}
