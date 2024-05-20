package com.atguigu.kafka.test.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerTest {
    public static void main(String[] args) {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // NOTE: Super important here, only difference from the producer
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "atguigu");


        // TODO: create consumer object
        KafkaConsumer<String, String> consumer = new KafkaConsumer(configMap);

        // TODO: subscriber the consumer
        consumer.subscribe(Collections.singletonList("test"));

//        // TODO: consumer pull(poll) data from kafka buffer
//        final ConsumerRecords<String, String> datas = consumer.poll(100);
//        for (ConsumerRecord<String, String> data : datas) {
//            System.out.println(data);
//        }

         // TODO: close kafka consumer
//        consumer.close();

        while (true){
            final ConsumerRecords<String, String> datas = consumer.poll(100);
            for (ConsumerRecord<String, String> data : datas) {
                System.out.println(data);
            }
        }
    }
}
