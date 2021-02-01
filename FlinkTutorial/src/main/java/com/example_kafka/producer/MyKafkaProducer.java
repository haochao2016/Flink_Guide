package com.example_kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyKafkaProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.121.71:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        for (int i= 0; i< 10; i++){
            kafkaProducer.send(new ProducerRecord<>("first", "f-" + Integer.toString(i), "f-v-"+ Integer.toString(i)));
//            if below code is used, the producer is sync
//            kafkaProducer.send(new ProducerRecord<>("first", "f-" + Integer.toString(i), "f-v-"+ Integer.toString(i))).get();
        }
        kafkaProducer.close();

        /*try {
            Thread.sleep(11);  // the edge is 10ms
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }
}
