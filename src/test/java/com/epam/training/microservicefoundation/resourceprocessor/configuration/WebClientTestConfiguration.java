package com.epam.training.microservicefoundation.resourceprocessor.configuration;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@TestConfiguration
public class WebClientTestConfiguration {
    @Value("${web-client.connection.timeout}")
    private String connectionTimeout;
    @Value("${web-client.response.timeout}")
    private String responseTimeout;
    @Value("${web-client.read.timeout}")
    private String readTimeout;
    @Value("${web-client.write.timeout}")
    private String writeTimeout;

    @Bean
    public HttpClient httpClient() {
        return  HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Integer.parseInt(connectionTimeout))
                .responseTimeout(Duration.ofMillis(Long.parseLong(responseTimeout)))
                .doOnConnected(connection ->
                        connection.addHandlerFirst(new ReadTimeoutHandler(Long.parseLong(readTimeout),
                                        TimeUnit.MILLISECONDS))
                                .addHandlerLast(new WriteTimeoutHandler(Long.parseLong(writeTimeout),
                                        TimeUnit.MILLISECONDS)));
    }

    @Bean
    @ConditionalOnProperty(prefix = "resource-service", name = "endpoint")
    public WebClient resourceServiceWebClient(@Value("${resource-service.endpoint}") String endpoint) {
        return WebClient.builder()
                .baseUrl(endpoint)
                .clientConnector(new ReactorClientHttpConnector(httpClient()))
                .build();
    }

    @Bean
    @ConditionalOnProperty(prefix = "song-service", name = "endpoint")
    public WebClient songServiceWebClient(@Value("${song-service.endpoint}") String endpoint) {
        return WebClient.builder()
                .baseUrl(endpoint)
                .clientConnector(new ReactorClientHttpConnector(httpClient()))
                .build();
    }
}
