package com.example.hellokafka.opensearch;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.opensearch.client.RequestOptions.DEFAULT;

public class OpenSearchConsumer {

    public static final String INDEX_NAME = "wikimedia";
    public static final String TOPIC_NAME = "wikimedia.recentchange";
    private static URI connectionUri = URI.create("http://localhost:9200");

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws IOException {

        var client = createOpenSearchClient(connectionUri);
        var consumer = createKafkaConsumer();

        try (client; consumer) {

            createIndexIfNeeded(client);

            consumer.subscribe(Collections.singleton(TOPIC_NAME));

            while (true) {
                var records = consumer.poll(Duration.ofMillis(3000));
                var count = records.count();
                log.info("received {} records", count);

                for (var record : records) {
                    // send the record into the index!1
                    try {

                        var indexRequest = new IndexRequest(INDEX_NAME)
                                .source(record.value(), XContentType.JSON);
                        var response = client.index(indexRequest, DEFAULT);
                        log.info("inserted into index with id {}", response.getId());

                    } catch (Exception e) {
                        log.error("saving to index failed: {}", e.getMessage());
                    }

                }
            }
        }
    }

    private static void createIndexIfNeeded(RestHighLevelClient client) throws IOException {
        var indexExists = client.indices().exists(new GetIndexRequest(INDEX_NAME), DEFAULT);

        if (!indexExists) {
            var createIndexRequest = new CreateIndexRequest(INDEX_NAME);
            client.indices().create(createIndexRequest, DEFAULT);
            log.info("Index '{}' created.", INDEX_NAME);
        } else {
            log.info("Index '{}' already exists.", INDEX_NAME);
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        var properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, "consumer-opensearch-demo");
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(properties);
    }

    private static RestHighLevelClient createOpenSearchClient(URI uri) {

        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), "http"))
        );

    }
}
