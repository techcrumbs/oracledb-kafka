package com.oracle.health.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.List;

public class SampleConsumer<T> implements Runnable, AutoCloseable {

    private final Consumer<String, T> consumer;
    private final String topic;
    private final int expectedMessages;

    public SampleConsumer(Consumer<String, T> consumer, String topic, int expectedMessages) {
        this.consumer = consumer;
        this.topic = topic;
        this.expectedMessages = expectedMessages;
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(topic));
        int consumedRecords = 0;
        while (true) {
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMinutes(1));
            System.out.println("Consumed records: " + records.count());
            consumedRecords += records.count();
            if (consumedRecords >= expectedMessages) {
                return;
            }
            processRecords(records);
            // Commit records when done processing.
            consumer.commitSync();
        }
    }

    private void processRecords(ConsumerRecords<String, T> records) {
        // Application implementation of record processing.
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }

}
