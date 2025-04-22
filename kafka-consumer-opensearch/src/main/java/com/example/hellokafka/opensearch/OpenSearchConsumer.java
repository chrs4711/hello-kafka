package com.example.hellokafka.opensearch;

import com.google.gson.JsonParser;
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
                // poll triggers the autocommit, unless we disable it
                var records = consumer.poll(Duration.ofMillis(3000));
                var count = records.count();
                log.info("received {} records", count);

                for (var record : records) {
                    try {
                        // We could receive the same message twice, so we need to introduce idempotency.
                        // For that, we take advantage of the fact that opensearch makes no difference between
                        // creation and update. As long as we use the same ID per document, we're good.
                        var indexRequest = new IndexRequest(INDEX_NAME)
                                .source(record.value(), XContentType.JSON)
                                .id(extractId(record.value()));

                        var response = client.index(indexRequest, DEFAULT);
                        log.info("inserted into index with id {}", response.getId());

                    } catch (Exception e) {
                        log.error("saving to index failed: {}", e.getMessage());
                    }
                }

                if (!records.isEmpty()) {
                    // commit the offsets
                    // as an alternativ, leave the default setting `enable.auto.commit` at true
                    consumer.commitSync();
                    log.info("offsets have been committed");
                }

            }
        }
    }

    /**
     * We could also create our own id based on a combination of record.topic(), record.partition(), record.offset().
     * However, our incoming data already has a key, so we take that!
     */
    private static String extractId(String json) {

        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();

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
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false"); // !! auto commit abschalten!1

        return new KafkaConsumer<>(properties);
    }

    private static RestHighLevelClient createOpenSearchClient(URI uri) {

        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), "http"))
        );

    }
}
