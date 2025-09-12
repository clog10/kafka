package com.clog10.kafka.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Clog10Consumer {

    private static final Logger log = LoggerFactory.getLogger(Clog10Consumer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "clog10-group");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList("clog10-topic"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for(var consumerRecord: consumerRecords){
                    log.info("Offset = {}, Partition = {}, Value = {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.value());
                }
            }
        }

    }

}
