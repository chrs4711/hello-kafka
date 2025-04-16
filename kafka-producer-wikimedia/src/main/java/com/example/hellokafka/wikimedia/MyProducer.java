package com.example.hellokafka.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

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

        producer = new KafkaProducer<>(properties);
    }

    void sendIt(String payload) {
        log.info("Sending not implemented yet");
        // producer.send(new ProducerRecord<>(topic, payload));
    }

    void closeIt() {
        producer.close();
    }

}
