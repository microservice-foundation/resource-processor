package com.epam.training.microservicefoundation.resourceprocessor.contract.resourceservice;

import com.epam.training.microservicefoundation.resourceprocessor.client.ResourceServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.ResourceServiceClientTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.RetryTemplateTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.WebClientTestConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.contract.stubrunner.spring.AutoConfigureStubRunner;
import org.springframework.cloud.contract.stubrunner.spring.StubRunnerProperties;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@AutoConfigureStubRunner(stubsMode = StubRunnerProperties.StubsMode.LOCAL,
        ids = "com.epam.training:resource-service:+:stubs:1001")
@ContextConfiguration(classes = {
        WebClientTestConfiguration.class,
        ResourceServiceClientTestConfiguration.class,
        RetryTemplateTestConfiguration.class
})
@TestPropertySource(locations = "classpath:application.properties")
@DirtiesContext
class ResourceServiceControllerTest {

    @Autowired
    private ResourceServiceClient resourceServiceClient;

    @BeforeAll
    static void initialize() {
        System.setProperty("resource-service.endpoint", "http://localhost:1001/api/v1");
    }

    @Test
    void shouldGetResourceById() {
        Optional<File> optionalFile = resourceServiceClient.getById(123L);
        assertTrue(optionalFile.isPresent());
    }

    @Test
    void shouldReturnEmptyWhenGetNonexistentResourceById() {
        Optional<File> optionalFile = resourceServiceClient.getById(1999L);
        assertTrue(optionalFile.isEmpty());
    }
}
