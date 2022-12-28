package com.epam.training.microservicefoundation.resourceprocessor.configuration;

import com.epam.training.microservicefoundation.resourceprocessor.client.ResourceServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.domain.ResourceType;
import com.epam.training.microservicefoundation.resourceprocessor.service.FileConvertor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.HashMap;
import java.util.Map;


@Configuration
@RefreshScope
public class ResourceServiceClientConfiguration {
    @Value("${resource-service.accept.mime-type}")
    private String acceptMimeType;

    private Map<String, String> resourceClientHeader() {
        Map<String, String> header = new HashMap<>();
        header.put(HttpHeaders.ACCEPT, acceptMimeType);
        return header;
    }

    private FileConvertor fileConvertor() {
        return new FileConvertor(ResourceType.getResourceTypeByMimeType(acceptMimeType));
    }

    @Bean
    public ResourceServiceClient resourceServiceClient(RetryTemplate retryTemplate, WebClient webClient) {
        return new ResourceServiceClient(resourceClientHeader(), fileConvertor(), retryTemplate, webClient);
    }
}
