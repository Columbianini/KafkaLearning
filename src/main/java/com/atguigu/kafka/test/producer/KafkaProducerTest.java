package com.atguigu.kafka.test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerTest {
    public static void main(String[] args) {
        // TODO: create config
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // TODO: serialize the producer data (k, v)
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // TODO: Create  Producer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

//        // TODO: Create Record
//        ProducerRecord<String, String> record = new ProducerRecord<>("test", "key", "value");
//        // TODO: Send record to Kafka(Provider) through Producer object
//        producer.send(record);

        for (int i=0;i<10;i++){
            ProducerRecord<String, String> record = new ProducerRecord<>("test", "key"+i, "value"+i);
            producer.send(record);
        }

        // TODO: Destroy Producer object
        producer.close();
    }
}
