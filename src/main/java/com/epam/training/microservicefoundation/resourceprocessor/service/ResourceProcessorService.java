package com.epam.training.microservicefoundation.resourceprocessor.service;

import com.epam.training.microservicefoundation.resourceprocessor.client.ResourceServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.client.SongServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.domain.ResourceRecord;
import com.epam.training.microservicefoundation.resourceprocessor.domain.SongRecord;
import com.mpatric.mp3agic.ID3v1;
import com.mpatric.mp3agic.ID3v2;
import com.mpatric.mp3agic.InvalidDataException;
import com.mpatric.mp3agic.Mp3File;
import com.mpatric.mp3agic.UnsupportedTagException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.time.LocalDateTime;
import java.util.Optional;

@Service
@Transactional(readOnly = true)
public class ResourceProcessorService {
    private static final Logger log = LoggerFactory.getLogger(ResourceProcessorService.class);
    private final Validator<ResourceRecord> resourceRecordValidator;
    private final ResourceServiceClient resourceServiceClient;
    private final SongServiceClient songServiceClient;

    @Autowired
    public ResourceProcessorService(Validator<ResourceRecord> resourceRecordValidator,
                                    ResourceServiceClient resourceServiceClient,
                                    SongServiceClient songServiceClient) {
        this.resourceRecordValidator = resourceRecordValidator;
        this.resourceServiceClient = resourceServiceClient;
        this.songServiceClient = songServiceClient;
    }


    public boolean processResource(ResourceRecord resourceRecord) {
        log.info("Processing resource record: {}", resourceRecord);

        if(!resourceRecordValidator.validate(resourceRecord)) {
            IllegalArgumentException ex = new IllegalArgumentException(String.format("Resource record '%s' " +
                    "was not validated, check your required parameters", resourceRecord));

            log.error("Resource record '{}' was not valid to parse metadata\nreason:", resourceRecord, ex);
            throw ex;
        }

        Optional<File> fileOptional = resourceServiceClient.getById(resourceRecord.getId());
        boolean isProcessed = fileOptional.map(file -> {
            try {
                return processFile(resourceRecord.getId(), file);
            } catch (InvalidDataException | UnsupportedTagException | IOException ex) {
                log.error("MP3 file processing failed for resource '{}' ", resourceRecord, ex);
                return null;
            }
        })
        .map(songServiceClient::post)
        .isPresent();

        fileOptional.ifPresent(this::removeIfExists);
        return isProcessed;
    }

    private SongRecord processFile(long resourceId, File file) throws InvalidDataException, UnsupportedTagException, IOException {
        Mp3File mp3File = new Mp3File(file);
        String duration = String.format("%1d:%2d", mp3File.getLengthInSeconds() / 60, mp3File.getLengthInSeconds() % 60);
        if(mp3File.hasId3v1Tag()) {
            return new SongRecord.Builder(resourceId, mp3File.getId3v1Tag().getTitle(),
                    duration)
                    .artist(mp3File.getId3v1Tag().getArtist())
                    .album(mp3File.getId3v1Tag().getAlbum())
                    .year(Integer.parseInt(mp3File.getId3v1Tag().getYear()))
                    .build();
        } else if (mp3File.hasId3v2Tag()) {
            return new SongRecord.Builder(resourceId, mp3File.getId3v2Tag().getTitle(),
                    duration)
                    .artist(mp3File.getId3v2Tag().getArtist())
                    .album(mp3File.getId3v2Tag().getAlbum())
                    .year(Integer.parseInt(mp3File.getId3v2Tag().getYear()))
                    .build();
        } else {
            return new SongRecord.Builder(resourceId, "No name: " + LocalDateTime.now(), duration).build();
        }
    }

    private void removeIfExists(File file) {
        log.info("Removing file '{}'", file);
        try {
            Files.deleteIfExists(file.toPath());
        } catch (NoSuchFileException exception) {
            log.error("File '{}' does not exit", file.toPath(), exception);
        } catch (DirectoryNotEmptyException exception) {
            log.error("Directory '{}' is not empty ", file.toPath(), exception);
        } catch (IOException exception) {
            log.error("File permission might be caught by user", exception);
        }
    }
}
