package com.clog10.kafka.kafka.callbacks;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Clog10CallbackProducer {

    public static final Logger log = LoggerFactory.getLogger(Clog10CallbackProducer.class);

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Broker de Kafka
        props.put("acks", "all"); // Acnknoeledgment level
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "10");

        try (Producer<String, String> producer = new KafkaProducer<>(props);) {
            for (int i = 0; i < 100000; i++) {
                producer.send(
                        new ProducerRecord<>("clog10-topic", String.valueOf(i),
                                String.valueOf(i).concat("-clog10-value")),
                        (metadata, exception) -> {
                            if (exception != null) {
                                log.info("There was an error {}", exception.getMessage());
                            }
                            log.info("Offset = {}, Partition = {}, Topic = {} ", metadata.offset(),
                                    metadata.partition(), metadata.topic());
                        });
            }
            producer.flush();
        }

        log.info("Processing time: {} ms", (System.currentTimeMillis() - startTime));

    }

}
