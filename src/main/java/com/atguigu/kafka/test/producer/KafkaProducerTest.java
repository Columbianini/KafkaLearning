package com.atguigu.kafka.test.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // TODO: create config
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // TODO: serialize the producer data (k, v)
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ValueInterceptorTest.class.getName());
        configMap.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyKafkaPartitioner.class.getName());
        configMap.put(ProducerConfig.ACKS_CONFIG, "-1"); // https://kafka.apache.org/documentation/#producerconfigs_acks

        // TODO: Create  Producer object
        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

//        // TODO: Create Record
//        ProducerRecord<String, String> record = new ProducerRecord<>("test", "key", "value");
//        // TODO: Send record to Kafka(Provider) through Producer object
//        producer.send(record);

        for (int i=0;i<10;i++){
            ProducerRecord<String, String> record = new ProducerRecord<>("test", "key"+i, "value"+i);
            Future<RecordMetadata> result = producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("Data sent successfully!" + recordMetadata);
                }
            });
            System.out.println("Data sent");
            result.get(); // wait/block the main threads until the end of current threads and then continue the main threads
        }


        // TODO: Destroy Producer object
        producer.close();
    }
}
