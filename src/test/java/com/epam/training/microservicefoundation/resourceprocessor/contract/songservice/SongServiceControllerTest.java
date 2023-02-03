package com.epam.training.microservicefoundation.resourceprocessor.contract.songservice;

import com.epam.training.microservicefoundation.resourceprocessor.client.SongServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.RetryTemplateTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.SongServiceClientTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.WebClientTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.model.SongRecord;
import com.epam.training.microservicefoundation.resourceprocessor.model.SongRecordId;
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

import static org.springframework.test.util.AssertionErrors.assertNotNull;

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
        SongRecord songRecord = new SongRecord.Builder(1L, "New office", "03:22")
                .artist("John Kennedy")
                .album("ASU")
                .year(1999).build();
        SongRecordId songRecordId = songServiceClient.post(songRecord);

        Assertions.assertNotNull(songRecordId);
        Assertions.assertEquals(199L, songRecordId.getId());
    }

    @Test
    void shouldReturnBadRequestWhenSaveSongMetadata() {
        SongRecord songRecord = new SongRecord.Builder(1L, "New office", "03:22")
                .artist("John Kennedy")
                .album("ASU")
                .year(2099).build();
        SongRecordId post = songServiceClient.post(songRecord);
        Assertions.assertNull(post);
    }
}
