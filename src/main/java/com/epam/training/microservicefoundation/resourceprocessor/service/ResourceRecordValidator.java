package com.epam.training.microservicefoundation.resourceprocessor.service;

import com.epam.training.microservicefoundation.resourceprocessor.model.ResourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ResourceRecordValidator implements Validator<ResourceRecord>{
    private static final Logger log = LoggerFactory.getLogger(ResourceRecordValidator.class);
    @Override
    public boolean validate(ResourceRecord input) {
        log.info("Validating a resource record '{}'", input);
        if(input == null) {
            return false;
        }

        return input.getId() > 0L;
    }
}
