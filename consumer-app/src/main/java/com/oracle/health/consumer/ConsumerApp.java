package com.oracle.health.consumer;


import com.oracle.health.messaging.Constants;
import com.oracle.health.messaging.OKafkaProperties;
import org.apache.kafka.clients.consumer.Consumer;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.oracle.okafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class ConsumerApp {

    private static final String TOPIC = "TXEQ_TEST1";

    public static void main(String[] args) {

        String ojdbcPropertiesFileDir = System.getProperty(Constants.OJDBC_PROPERTIES_FILE_DIR_PROP_KEY);

        // Create the Consumer.
        Properties consumerProps = OKafkaProperties.getLocalConnectionProps(ojdbcPropertiesFileDir, Constants.ORACLE_DB_PORT);
        consumerProps.put("group.id", "MY_CONSUMER_GROUP1");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("max.poll.records", 1);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (Consumer<String, String> okafkaConsumer = new KafkaConsumer<>(consumerProps);
             SampleConsumer<String> sampleConsumer = new SampleConsumer<>(okafkaConsumer, TOPIC, 100)
        ) {
            var t = new Thread(sampleConsumer);
            t.start();
            System.out.println("Waiting for consumer thread to complete");
            t.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
