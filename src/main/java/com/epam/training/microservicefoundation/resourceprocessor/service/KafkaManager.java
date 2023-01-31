package com.epam.training.microservicefoundation.resourceprocessor.service;

import com.epam.training.microservicefoundation.resourceprocessor.model.ResourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaManager {
    private final Logger log = LoggerFactory.getLogger(KafkaManager.class);

    private final ResourceProcessorService processorService;

    @Autowired
    public KafkaManager(ResourceProcessorService processorService) {
        this.processorService = processorService;
    }

    @KafkaListener(topics = "resources")
    public void listen(ResourceRecord resourceRecord, Acknowledgment acknowledgment) {
        log.info("A message received from the resources topic': {}", resourceRecord);
        if(processorService.processResource(resourceRecord)) {
            acknowledgment.acknowledge();
        }
    }
}
