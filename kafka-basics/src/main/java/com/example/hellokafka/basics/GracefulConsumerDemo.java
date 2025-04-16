package com.example.hellokafka.basics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * If this one is run with multiple instances, we can watch the partitions get re-assigned to the individual
 * consumers within the consumer group, e.g. using
 * * kafka-consumer-groups ... --describe --group my-app
 * * turning on 'info' logging for `org.apache.kafka.clients.consumer`
 */
public class GracefulConsumerDemo {

    private static final String groupId = "my-app";
    private static final String topic = "demo_java";

    private static final Logger logger = LoggerFactory.getLogger(GracefulConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("The graceful consumer is starting!");

        var properties = new Properties();
        // connection properties
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        // auto.offset.reset: can be none/earliest/latest
        // none: no existing consumer group? Fail
        // earliest: same as --from-beginning
        // latest: only new stuff.
        properties.setProperty("auto.offset.reset", "earliest");

        try (var consumer = new KafkaConsumer<String, String>(properties)) {

            var mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    logger.info("Waking up the consumer");
                    consumer.wakeup(); // causes the consumer to throw a wakeup exception

                    // join the main thread to allow it to finish. Otherwise, we would just die.
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            try {

                consumer.subscribe(List.of(topic));

                while (true) {
                    // logger.info("polling ...");
                    var records = consumer.poll(Duration.ofMillis(1000));
                    records.forEach(GracefulConsumerDemo::logMessage);
                }

            } catch (WakeupException e) {
                logger.info("Consumer is shutting down");
            } catch (Exception e) {
                logger.error("Unexpected exception", e);
            } finally {
                // closing the consumer allows to commit the offsets and rebalance the consumer group gracefully
                logger.info("Shutting down the consumer gracefully...");
                consumer.close();
            }

        }

    }

    private static void logMessage(ConsumerRecord<String, String> record) {
        logger.info("Key: {}, Value: {}, Partition: {}, Offset: {}",
                record.key(),
                record.value(),
                record.partition(),
                record.offset()
        );
    }
}
