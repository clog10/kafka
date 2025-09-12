package com.clog10.kafka.kafka.producers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Clog10Producer {

    public static final Logger log = LoggerFactory.getLogger(Clog10Producer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Broker de Kafka
        props.put("acks", "all"); // Acnknoeledgment level
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props);) {
            for (int i = 0; i < 10000; i++) {
                producer.send(
                        new ProducerRecord<>("clog10-topic", String.valueOf(i).concat("clog10-key"), "clog10-value"))
                        .get();
            }
            producer.flush();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Message producer interrupted ", e);
        }

    }

}
