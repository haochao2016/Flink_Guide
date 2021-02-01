package com.example_kafka.producer;

import org.apache.kafka.clients.producer.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

import java.util.Properties;

public class CallBackProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "192.168.121.71:9092");
        props.put(ACKS_CONFIG, "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(PARTITIONER_CLASS_CONFIG, "com.example_kafka.producer.partitioner.MyKafkaPartitioner");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        for (int i= 0; i< 10; i++){
//            kafkaProducer.send(new ProducerRecord<>("first", "f-" + Integer.toString(i), "f-v-"+ Integer.toString(i)));
            kafkaProducer.send(new ProducerRecord<>("first", "f-" + Integer.toString(i), "f-v-" + Integer.toString(i)),
                    (metadata, exception) -> {
                        if (exception == null)
                            System.out.println(metadata.partition() + "--" + metadata.offset());
                    });
        }
        kafkaProducer.close();
    }
}
