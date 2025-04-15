package com.example.hellokafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerKeysDemo {

    private static final String topic = "demo_java";
    private static final int messageCount = 10;

    private static final Logger logger = LoggerFactory.getLogger(ProducerKeysDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("The producer with keys has started!");

        var properties = new Properties();
        // connection properties
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // check the logs to see that messages with the same key go into the same partition!
        try (var producer = new KafkaProducer<String, String>(properties)) {

            IntStream.range(0, 3).forEach(r -> {

                IntStream.range(0, messageCount).forEach(i -> {
                    var record = recordFor("hello world", i);
                    producer.send(record, (metadata, exception) -> {
                        onCompletion(metadata, exception, record.key());
                    });
                    logger.info("Sent key: {}, value: {}", record.key(), record.value());
                });

            });

        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }

    private static ProducerRecord<String, String> recordFor(String message, int i) {
        var key = "id_" + i;
        var value = message + " " + i;
        return new ProducerRecord<>(topic, key, value);
    }

    /**
     * Executed everytime a record was successfully sent or an exception occurred.
     */
    private static void onCompletion(RecordMetadata metadata, Exception exception, String key) {

        if (exception == null) {
            logger.info("Got metadata: Topic: {}, Key: {}, Partition: {}, Offset: {}, Timestamp: {}",
                    metadata.topic(),
                    key,
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp()
            );
        } else {
            logger.error("Error sending message: {}", exception.getMessage());
        }

    }
}
