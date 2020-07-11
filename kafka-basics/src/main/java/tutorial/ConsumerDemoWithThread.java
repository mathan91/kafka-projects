package tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {
    }

    private void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        System.out.println("Hello world");
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";
        CountDownLatch countDownLatch = new CountDownLatch(1);
        logger.info("Creating the consumer thread");
        Runnable myConsumer = new ConsumerThread(countDownLatch, bootstrapServer, groupId, topic);
        Thread myThread = new Thread(myConsumer);
        myThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumer).shutDown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                logger.info("Application has exited");
            }
        }));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("error in invokin");
        } finally {
            logger.info("application is closing");
        }
    }

    public class ConsumerThread implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
        public ConsumerThread(CountDownLatch latch, String bootstrapServer, String groupId, String topic) {
            this.latch = latch;
            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create a consumer
            consumer = new KafkaConsumer<String, String>(properties);
            // subscribe
            consumer.subscribe(Collections.singleton(topic));
            // poll for new data
        }

        @Override
        public void run () {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record : records) {
                        logger.info("key: " + record.value());
                        logger.info("partition: " + record.partition());
                        logger.info("value: " + record.value());
                        logger.info("Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown () {
            // the wakeup method is a special method to interrupt consumer.poll()
            consumer.wakeup();
        }
    }
}

