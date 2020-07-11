package tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithSeekAndAssign {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithSeekAndAssign.class);
        System.out.println("Hello world");
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fifth-application";
        String topic = "first_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition partitionToRead = new TopicPartition(topic, 0);
        long offsetToRead = 15L;
        consumer.assign(Arrays.asList(partitionToRead));

        // seek
        consumer.seek(partitionToRead, offsetToRead);

        int numOfMessages = 5;
        boolean keepOnReading = true;
        int numOfMessagesRead = 0;

        // poll for new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                numOfMessagesRead++;
                logger.info("key: " + record.value());
                logger.info("partition: " + record.partition());
                logger.info("value: " + record.value());
                logger.info("Offset: "+ record.offset());
                if (numOfMessagesRead >= numOfMessages) {
                    keepOnReading = false;
                    break;
                }
            }
        }
    }
}
