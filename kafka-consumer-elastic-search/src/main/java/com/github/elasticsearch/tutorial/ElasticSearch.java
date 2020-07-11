package com.github.elasticsearch.tutorial;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.openmbean.TabularData;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearch {

    public static RestHighLevelClient createClient() {
        String hostname = "kafka-course-8486591081.ap-southeast-2.bonsaisearch.net";
        String username = "gy379avhgh";
        String password = "889ogjjrjc";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearch.class);
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = consumer();
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            logger.info("Received the following no of messages: " + records.count());
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord record : records) {
//                String id1 = extractIdFromTweets(record.toString());
                IndexRequest request = new IndexRequest("twitter", "tweets").source(record.value(), XContentType.JSON);
                bulkRequest.add(request);
                logger.info("Offset: "+ record.offset());
                logger.info("key: " + record.value());
                logger.info("partition: " + record.partition());
                logger.info("value: " + record.value());
                }
                if (records.count() > 100) {
                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
    //                IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
    //                String id = indexResponse.getId();
                    logger.info("The tweet is generated : ");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            logger.info("committing offsets");
            consumer.commitSync();
            logger.info("Offsets committed");
            }

//        client.close();
    }
    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweets(String record) {
        return jsonParser.parse(record)
                .getAsJsonObject().get("id_str")
                .getAsString();
    }

    public static KafkaConsumer<String, String> consumer() {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
}
