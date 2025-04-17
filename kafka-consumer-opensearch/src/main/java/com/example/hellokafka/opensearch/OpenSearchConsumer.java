package com.example.hellokafka.opensearch;

import org.apache.http.HttpHost;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.opensearch.client.RequestOptions.DEFAULT;

public class OpenSearchConsumer {

    public static final String INDEX_NAME = "wikimedia";
    private static URI connectionUri = URI.create("http://localhost:9200");

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws IOException {

        var client = createOpenSearchClient(connectionUri);

        try (client) {

            createIndexIfNeeded(client);

        }


        // create the kafka consumer

        // logic

        // cleanup
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

    private static RestHighLevelClient createOpenSearchClient(URI uri) {

        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), "http"))
        );

    }
}
