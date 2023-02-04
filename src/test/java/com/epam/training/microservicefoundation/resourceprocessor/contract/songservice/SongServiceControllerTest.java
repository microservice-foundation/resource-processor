package com.epam.training.microservicefoundation.resourceprocessor.contract.songservice;

import com.epam.training.microservicefoundation.resourceprocessor.client.SongServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.RetryTemplateTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.SongServiceClientTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.WebClientTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.model.SongMetadata;
import com.epam.training.microservicefoundation.resourceprocessor.model.SongRecord;
import org.junit.jupiter.api.Assertions;
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

@ExtendWith(SpringExtension.class)
@AutoConfigureStubRunner(stubsMode = StubRunnerProperties.StubsMode.LOCAL,
        ids = "com.epam.training:song-service:+:stubs:1001")
@ContextConfiguration(classes = {
        WebClientTestConfiguration.class,
        SongServiceClientTestConfiguration.class,
        RetryTemplateTestConfiguration.class
})
@TestPropertySource(locations = "classpath:application.properties")
@DirtiesContext
class SongServiceControllerTest {

    @Autowired
    private SongServiceClient songServiceClient;

    @BeforeAll
    static void initialize() {
        System.setProperty("song-service.endpoint", "http://localhost:1001/api/v1");
    }

    @Test
    void shouldSaveSongMetadata() {
        SongMetadata songMetadata = new SongMetadata.Builder(1L, "New office", "03:22")
                .artist("John Kennedy")
                .album("ASU")
                .year(1999).build();
        SongRecord songRecord = songServiceClient.post(songMetadata);

        Assertions.assertNotNull(songRecord);
        Assertions.assertEquals(199L, songRecord.getId());
    }

    @Test
    void shouldReturnBadRequestWhenSaveSongMetadata() {
        SongMetadata songMetadata = new SongMetadata.Builder(1L, "New office", "03:22")
                .artist("John Kennedy")
                .album("ASU")
                .year(2099).build();
        SongRecord post = songServiceClient.post(songMetadata);
        Assertions.assertNull(post);
    }
}
