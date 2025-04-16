package com.example.hellokafka.basics;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final String groupId = "my-app";
    private static final String topic = "demo_java";

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("The basic consumer is starting!");

        var properties = new Properties();
        // connection properties
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        // can be none/earliest/latest
        // none: no existing consumer group? Fail
        // earliest: same as --from-beginning
        // latest: only new stuff.
        properties.setProperty("auto.offset.reset", "earliest");

        try (var consumer = new KafkaConsumer<String, String>(properties)) {

            consumer.subscribe(List.of(topic));

            while (true) {
                logger.info("polling ...");
                var records = consumer.poll(Duration.ofMillis(1000));

                records.forEach(record -> {
                    logger.info("Key: {}, Value: {}, Partition: {}, Offset: {}",
                            record.key(),
                            record.value(),
                            record.partition(),
                            record.offset()
                    );
                });
            }
        }

    }
}
