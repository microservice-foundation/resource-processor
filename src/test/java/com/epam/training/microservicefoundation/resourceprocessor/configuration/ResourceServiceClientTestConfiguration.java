package com.epam.training.microservicefoundation.resourceprocessor.configuration;

import com.epam.training.microservicefoundation.resourceprocessor.client.ResourceServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.model.ResourceType;
import com.epam.training.microservicefoundation.resourceprocessor.service.FileConvertor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.HashMap;
import java.util.Map;


@TestConfiguration
public class ResourceServiceClientTestConfiguration {
    @Value("${resource-service.accept.mime-type}")
    private String acceptMimeType;

    @Bean
    public Map<String, String> resourceClientHeader() {
        Map<String, String> header = new HashMap<>();
        header.put(HttpHeaders.ACCEPT, acceptMimeType);
        return header;
    }

    @Bean
    public FileConvertor fileConvertor() {
        return new FileConvertor(ResourceType.getResourceTypeByMimeType(acceptMimeType));
    }

    @Bean
    public ResourceServiceClient resourceServiceClient(RetryTemplate retryTemplate, WebClient resourceServiceWebClient) {
        return new ResourceServiceClient(resourceClientHeader(), fileConvertor(), retryTemplate, resourceServiceWebClient);
    }
}
