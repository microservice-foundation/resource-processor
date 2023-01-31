package com.epam.training.microservicefoundation.resourceprocessor.common;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;


public class FakeKafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(FakeKafkaProducer.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Map<Class<?>, Pair<String, Function<Object, ProducerRecord<String, Object>>>> publicationTopics;

    public FakeKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate, Map<Class<?>, Pair<String, Function<Object,
            ProducerRecord<String, Object>>>> publicationTopics) {

        this.kafkaTemplate = kafkaTemplate;
        this.publicationTopics = publicationTopics;
    }

    public void publish(Object message) throws ExecutionException, InterruptedException {
        if(publicationTopics.containsKey(message.getClass())) {
            log.info("publishing {} message to kafka: {}", message.getClass().getName(), message);
            RecordMetadata metadata = kafkaTemplate.send(publicationTopics.get(message.getClass()).getRight().apply(message)).get().getRecordMetadata();

            log.info("RecordMetadata topic: {}, offset: {}, partition: {}",
                    metadata.topic(),
                    metadata.offset(),
                    metadata.partition());
        }
    }
}

