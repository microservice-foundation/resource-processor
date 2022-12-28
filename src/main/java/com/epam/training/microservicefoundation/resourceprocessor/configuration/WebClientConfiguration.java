package com.epam.training.microservicefoundation.resourceprocessor.configuration;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.loadbalancer.reactive.ReactorLoadBalancerExchangeFilterFunction;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
@RefreshScope
public class WebClientConfiguration {

    @Value("${api-gateway.endpoint}")
    private String apiGatewayEndpoint;
    @Value("${web-client.connection.timeout}")
    private String connectionTimeout;
    @Value("${web-client.response.timeout}")
    private String responseTimeout;
    @Value("${web-client.read.timeout}")
    private String readTimeout;
    @Value("${web-client.write.timeout}")
    private String writeTimeout;

    private HttpClient httpClient() {
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
    public WebClient webClient(ReactorLoadBalancerExchangeFilterFunction loadBalancerExchangeFilterFunction) {
        return WebClient.builder()
                .baseUrl(apiGatewayEndpoint)
                .filter(loadBalancerExchangeFilterFunction)
                .clientConnector(new ReactorClientHttpConnector(httpClient()))
                .build();
    }
}
