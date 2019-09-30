package com.github.fuokista.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallback {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);


        //String bootstrapServers = "127.0.0.1:9092";
        String bootstrapServers = "192.168.159.130:9092";

        //create Producer properties
        Properties properties = new Properties();
//        properties.setProperty("bootstrap_servers", bootstrapServers);               //da sito kafka
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        properties.setProperty("key.serializer", StringSerializer.class.getName());  //da sito kafka
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty("value.serializer", StringSerializer.class.getName()); //da sito kafka
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create the producer
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world" + Integer.toString(i));

            //send data - asynchronous
            producer.send(record, new Callback() {
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                            if (e == null) {
                                logger.info("Received new metadata: \n"
                                        + "Topic: " + recordMetadata.topic() + "\n"
                                        + "Partition: " + recordMetadata.partition() + "\n"
                                        + "Offset: " + recordMetadata.offset() + "\n"
                                        + "TimeStamp: " + recordMetadata.timestamp()
                                );
                            } else {
                                logger.error("Error while producing");
                            }

                        }
                    }
            );
        }
        // flush data
        producer.flush();
        ;

        // flush and close producer
        producer.close();


    }

}
