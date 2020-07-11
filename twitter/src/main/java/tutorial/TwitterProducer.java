package tutorial;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    String consumerKey = "yXaYmGVC5MTSQYpiThqPAMtRY";
    String consumerSecret = "AZhZqvDbCUtu9xJz00k4P5QF24XeBntmMxsXwVv2QuaxPdW4Yy";
    String secret = "7XFIvB3fW2wcva2bY6Md6uTqSkq7DU5RQ3F99F7uyOn9K";
    String token = "752906846-YHxD9lPTgSgd5tPuayI3YGTMIufNcuTqTCFL3wwk";
    List<String> terms = Lists.newArrayList("kafka", "master", "bitcoin", "real madrid");
    private TwitterProducer(){

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        Client client = createTwitterClient(msgQueue);
        client.connect();
        KafkaProducer<String, String> producer = createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Stopping application");
            logger.info("Shutting down the application");
            client.stop();
            logger.info("Closing producer");
            producer.close();
            logger.info("done");
        }));
        while(!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            logger.error("Some error" + e);
                        }
                    }
                });
            }
        }
        logger.info("End of Application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServer = "127.0.0.1:9092";
        // create producer properties;
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // good producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        // high throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */

        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track term
        hosebirdEndpoint.trackTerms(terms);
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        // Attempts to establish a connection.
        return builder.build();
    }
}
