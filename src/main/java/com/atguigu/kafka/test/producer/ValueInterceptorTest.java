package com.atguigu.kafka.test.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class ValueInterceptorTest implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // when sending message
        return new ProducerRecord<>(producerRecord.topic(), producerRecord.key(), producerRecord.value() + producerRecord.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // after sending message and server responded
    }

    @Override
    public void close() {
        // when producer closes
    }

    @Override
    public void configure(Map<String, ?> map) {
        // when creating producer
    }
}
