package com.clog10.kafka.kafka.transactional;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalProducer {

    public static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Broker de Kafka
        props.put("acks", "all"); // Acnknoeledgment level
        props.put("transactional.id", "clog10-producer-id"); // Acnknoeledgment level
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "10");

        try (Producer<String, String> producer = new KafkaProducer<>(props);) {
            try {
                producer.initTransactions();
                producer.beginTransaction();
                for (int i = 0; i < 10000; i++) {
                    producer.send(
                            new ProducerRecord<>("clog10-topic", String.valueOf(i).concat(": clog10-key"),
                                    String.valueOf(i).concat("-clog10-value")));
                    //if (i == 5000) {
                    //    throw new Exception("Unexpected error at 5000");
                    //}
                }
                producer.commitTransaction();
                producer.flush();
            } catch (Exception e) {
                log.error("Error during transaction, aborting", e);
                producer.abortTransaction();
            }
        }

        log.info("Processing time: {} ms", (System.currentTimeMillis() - startTime));

    }

}
