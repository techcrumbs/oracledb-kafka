package com.oracle.health.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.stream.Stream;

public class SampleProducer<T> implements Runnable, AutoCloseable {

    private final Producer<String, String> producer;
    private final String topic;
    private final Stream<T> inputs;

    public SampleProducer(Producer<String, String> producer, String topic, Stream<T> inputs) {
        this.producer = producer;
        this.topic = topic;
        this.inputs = inputs;
    }

    @Override
    public void run() {
        inputs.forEach(t -> {
            String data = "Produced value: " + t;
            System.out.println(data);
            producer.send(new ProducerRecord<>(topic, String.valueOf(t.hashCode()), data));
        });
    }

    @Override
    public void close() {
        if (this.producer != null) {
            producer.close();
        }
    }

}
