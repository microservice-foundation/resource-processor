package com.epam.training.microservicefoundation.resourceprocessor.client;

import com.epam.training.microservicefoundation.resourceprocessor.model.ResourceType;
import com.epam.training.microservicefoundation.resourceprocessor.service.Convertor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.io.File;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ResourceServiceClient {
    //TODO: change to reactive
    private static final Logger log = LoggerFactory.getLogger(ResourceServiceClient.class);
    private static final String RESOURCES = "/resources";
    private static final String ID = "/{id}";
    private final WebClient webClient;
    private final String acceptHeader;
    private final Convertor<File, Flux<DataBuffer>> convertor;
    private final RetryTemplate retryTemplate;
    public ResourceServiceClient(Map<String, String> headers, Convertor<File, Flux<DataBuffer>> convertor,
                                 RetryTemplate retryTemplate, WebClient webClient) {

        this.webClient = webClient;
        this.acceptHeader = headers.get(HttpHeaders.ACCEPT);
        this.convertor = convertor;
        this.retryTemplate = retryTemplate;
    }

    public Optional<File> getById(long id) {
        return retryTemplate.execute(context -> {
            Flux<DataBuffer> dataBufferFlux = webClient.get()
                .uri(uriBuilder -> uriBuilder.path(RESOURCES).path(ID).build(id))
                .accept(MediaType.valueOf(Objects.requireNonNull(ResourceType.getResourceTypeByMimeType(acceptHeader))
                        .getMimeType()))
                .retrieve()
                .bodyToFlux(DataBuffer.class);

            return Optional.ofNullable(convertor.covert(dataBufferFlux));
        }, context -> {
            log.error("Getting resource file by id '{}' failed after '{}' retry attempts", id, context.getRetryCount());
            return Optional.empty();
        });
    }

}
