package com.example.hellokafka.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Provides a method to send payload with a configured producer to a kafka topic.
 * The producer by default has some "safe" settings, e.g.:
 *
 *  acks = -1
 * 	delivery.timeout.ms = 120000
 * 	enable.idempotence = true
 * 	max.in.flight.requests.per.connection = 5
 * 	retries = 2147483647
 *
 * 	Another important setting is `min.insync.replicas`, which is set on the broker
 *
 */
public class MyProducer {

    private String bootstrapServers = "127.0.0.1:9092";
    private String topic = "wikimedia.recentchange";
    private Properties properties = new Properties();

    private static final Logger log = LoggerFactory.getLogger(MyProducer.class.getSimpleName());

    private KafkaProducer<String, String> producer;

    MyProducer() {
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // `linger.ms` specifies how long the producer will wait to fill a batch.
        properties.setProperty(LINGER_MS_CONFIG, "20");

        // when `batch.size` is reached, the producer will send the batch
        properties.setProperty(BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        // messages are compressed by the producer using the snappy algorithm
        properties.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");

        producer = new KafkaProducer<>(properties);
    }

    void sendIt(String payload) {
        log.info("Sending to broker");
        producer.send(new ProducerRecord<>(topic, payload), this::onCompletion);
    }

    private void onCompletion(RecordMetadata metadata, Exception e) {

        if (e != null) {
            log.error("Exception during send: {}", e.getMessage());
            return;
        }

        log.info("Got metadata: Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                metadata.timestamp()
        );
    }

    void closeIt() {
        producer.close();
    }

}
