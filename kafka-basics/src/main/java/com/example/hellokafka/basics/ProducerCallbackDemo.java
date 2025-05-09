package com.example.hellokafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerCallbackDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerCallbackDemo.class.getSimpleName());
    public static final int MESSAGE_COUNT = 30;

    public static void main(String[] args) {
        logger.info("The basic producer has started!");

        var properties = new Properties();
        // connection properties
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Smaller batch size to provoke switching of partitions!
        // The default batch size of kafka is 16kb
        properties.setProperty("batch.size", "400");

        try (var producer = new KafkaProducer<String, String>(properties)) {

            IntStream.range(0, 10).forEach(j -> {

                IntStream.range(0, MESSAGE_COUNT).forEach(i -> {
                    var record = new ProducerRecord<String, String>("demo_java", "hello world! " + j + "-" + i);
                    producer.send(record, ProducerCallbackDemo::onCompletion);
                });

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            });

        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }

    /**
     * Executed everytime a record was successfully sent or an exception occurred.
     */
    private static void onCompletion(RecordMetadata metadata, Exception exception) {

        if (exception == null) {
            logger.info("Sent message, Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp()
                    );
        } else {
            logger.error("Error sending message: {}", exception.getMessage());
        }

    }
}
