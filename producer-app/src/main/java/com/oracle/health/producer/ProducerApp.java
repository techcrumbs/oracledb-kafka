package com.oracle.health.producer;

import com.oracle.health.messaging.AdminUtil;
import com.oracle.health.messaging.Constants;
import com.oracle.health.messaging.OKafkaProperties;
import org.apache.kafka.clients.admin.NewTopic;
//import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import org.oracle.okafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerApp {

    private static final String TOPIC = "TXEQ_TEST1";

    public static void main(String[] args) {

        String ojdbcPropertiesFileDir = System.getProperty(Constants.OJDBC_PROPERTIES_FILE_DIR_PROP_KEY);

        Properties connectionProperties = OKafkaProperties
                .getLocalConnectionProps(ojdbcPropertiesFileDir, Constants.ORACLE_DB_PORT);
        NewTopic topic = new NewTopic(TOPIC, 1, (short) 0);

        // create topic
        AdminUtil.createTopicIfNotExists(connectionProperties, topic);
        System.out.println("Topic created: " + TOPIC);

        // create producer
        // Create the Producer.
        Properties producerProps = OKafkaProperties
                .getLocalConnectionProps(ojdbcPropertiesFileDir, Constants.ORACLE_DB_PORT);
        producerProps.put("enable.idempotence", "true");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try(Producer<String, String> okafkaProducer = new KafkaProducer<>(producerProps);
            SampleProducer<Integer> producer = new SampleProducer<>(
                    okafkaProducer, TOPIC, IntStream.range(0,100).boxed())
        ) {
            var t = new Thread(producer);
            t.start();
            t.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
