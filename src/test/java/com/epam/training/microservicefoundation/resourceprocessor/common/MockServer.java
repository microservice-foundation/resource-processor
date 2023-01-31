package com.epam.training.microservicefoundation.resourceprocessor.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.io.IOException;
import java.util.Map;

public final class MockServer {
    private final MockWebServer server;
    private final WebClient webClient;
    private final ObjectMapper mapper;

    private MockServer(HttpClient httpClient) {
        mapper = new ObjectMapper();
        server = new MockWebServer();
        webClient = WebClient.builder()
                .baseUrl(server.url("/api/v1").url().toString())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }

    public WebClient getWebClient() {
        return webClient;
    }

    public <T> void responseWithJson(HttpStatus status, T responseBody, Map<String, String> headers) {
        MockResponse response = new MockResponse();
        response.setResponseCode(status.value());
        response.setBody(toJson(responseBody));
        headers.forEach(response::addHeader);
        server.enqueue(response);
    }

    public void responseWithBuffer(HttpStatus status, Buffer responseBody, Map<String, String> headers) {
        MockResponse response = new MockResponse();
        response.setResponseCode(status.value());
        response.setBody(responseBody);
        headers.forEach(response::addHeader);
        server.enqueue(response);
    }

    private <T> String toJson(T value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void dispose() throws IOException {
        server.close();
    }

    public static MockServer newInstance(HttpClient httpClient) {
        return new MockServer(httpClient);
    }
}
