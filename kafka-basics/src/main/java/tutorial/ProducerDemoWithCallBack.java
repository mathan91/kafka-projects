package tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
        String bootstrapServer = "127.0.0.1:9092";
        // create producer properties;
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(properties);
        // create a producer record
        for (int i=0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world" + Integer.toString(i));
            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes everytime an exception is thrown or successfully sent
                    if (e == null) {
                        logger.info("Received new metadata \n " +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.partition() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while creating" + e);
                    }

                }
            });
        }
        producer.flush();
        producer.close();
    }
}
