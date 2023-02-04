package com.epam.training.microservicefoundation.resourceprocessor.client;

import com.epam.training.microservicefoundation.resourceprocessor.model.SongMetadata;
import com.epam.training.microservicefoundation.resourceprocessor.model.SongRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

public class SongServiceClient {
    //TODO: change to reactive
    private static final Logger log = LoggerFactory.getLogger(SongServiceClient.class);
    private static final String SONGS = "/songs";
    private final WebClient webClient;
    private final String acceptHeader;
    private final RetryTemplate retryTemplate;
    public SongServiceClient(Map<String, String> headers, RetryTemplate retryTemplate, WebClient webClient) {
        this.webClient = webClient;
        this.acceptHeader = headers.get(HttpHeaders.ACCEPT);
        this.retryTemplate = retryTemplate;
    }

    public SongRecord post(SongMetadata songMetadata) {
        return retryTemplate.execute(
                context -> webClient.post()
                    .uri(uriBuilder -> uriBuilder.path(SONGS).build())
                    .accept(MediaType.valueOf(acceptHeader))
                    .bodyValue(songMetadata)
                    .retrieve()
                    .bodyToMono(SongRecord.class)
                    .block(),
                context -> {
                    log.error("Sending post request to song service failed with '{}' retry attempts",
                            context.getRetryCount());
                    return null;
                });
    }

}
