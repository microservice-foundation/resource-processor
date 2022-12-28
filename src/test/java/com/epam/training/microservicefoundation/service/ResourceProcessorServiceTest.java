package com.epam.training.microservicefoundation.service;

import com.epam.training.microservicefoundation.resourceprocessor.client.ResourceServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.client.SongServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.domain.ResourceRecord;
import com.epam.training.microservicefoundation.resourceprocessor.domain.SongRecordId;
import com.epam.training.microservicefoundation.resourceprocessor.service.ResourceProcessorService;
import com.epam.training.microservicefoundation.resourceprocessor.service.ResourceRecordValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ResourceProcessorServiceTest {

    @Mock
    private ResourceServiceClient resourceServiceClient;
    @Mock
    private SongServiceClient songServiceClient;
    @Mock
    private ResourceRecordValidator resourceRecordValidator;
    private ResourceProcessorService service;

    @BeforeEach
    public void setup() {
        service = new ResourceProcessorService(resourceRecordValidator, resourceServiceClient, songServiceClient);
    }

    @Test
    void shouldProcessResource() throws IOException {
        ResourceRecord resourceRecord = new ResourceRecord(1L);
        File file = ResourceUtils.getFile("src/test/resources/files/mpthreetest.mp3");
        File testFile = ResourceUtils.getFile("src/test/resources/files/test.mp3");
        Files.copy(file.toPath(), testFile.toPath());

        when(resourceRecordValidator.validate(resourceRecord)).thenReturn(Boolean.TRUE);
        when(resourceServiceClient.getById(resourceRecord.getId())).thenReturn(Optional.of(testFile));
        when(songServiceClient.post(any())).thenReturn(new SongRecordId(1L));

        boolean isProcessed = service.processResource(resourceRecord);
        assertTrue(isProcessed);

        verify(resourceRecordValidator, times(1)).validate(resourceRecord);
        verify(resourceServiceClient, times(1)).getById(resourceRecord.getId());
        verify(songServiceClient, times(1)).post(any());
    }

    @Test
    void shouldThrowExceptionForValidationWhenProcessResource() throws FileNotFoundException {
        ResourceRecord resourceRecord = new ResourceRecord(0L);
        when(resourceRecordValidator.validate(resourceRecord)).thenReturn(Boolean.FALSE);

        assertThrows(IllegalArgumentException.class, () -> service.processResource(resourceRecord));

        verify(resourceRecordValidator, times(1)).validate(resourceRecord);
    }

    @Test
    void shouldReturnEmptyResourceWhenProcessResource() throws FileNotFoundException {
        ResourceRecord resourceRecord = new ResourceRecord(2L);
        when(resourceRecordValidator.validate(resourceRecord)).thenReturn(Boolean.TRUE);
        when(resourceServiceClient.getById(resourceRecord.getId())).thenReturn(Optional.empty());

        boolean isProcessed = service.processResource(resourceRecord);
        assertFalse(isProcessed);

        verify(resourceRecordValidator, times(1)).validate(resourceRecord);
        verify(resourceServiceClient, times(1)).getById(resourceRecord.getId());
    }

    @Test
    void shouldReturnEmptySongRecordIdWhenProcessResource() throws IOException {
        ResourceRecord resourceRecord = new ResourceRecord(1L);
        File file = ResourceUtils.getFile("src/test/resources/files/mpthreetest.mp3");
        File testFile = ResourceUtils.getFile("src/test/resources/files/test.mp3");
        Files.copy(file.toPath(), testFile.toPath());

        when(resourceRecordValidator.validate(resourceRecord)).thenReturn(Boolean.TRUE);
        when(resourceServiceClient.getById(resourceRecord.getId())).thenReturn(Optional.of(testFile));
        when(songServiceClient.post(any())).thenReturn(null);

        boolean isProcessed = service.processResource(resourceRecord);
        assertFalse(isProcessed);

        verify(resourceRecordValidator, times(1)).validate(resourceRecord);
        verify(resourceServiceClient, times(1)).getById(resourceRecord.getId());
        verify(songServiceClient, times(1)).post(any());
    }
}