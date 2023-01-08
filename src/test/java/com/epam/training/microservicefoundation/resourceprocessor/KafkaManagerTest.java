package com.epam.training.microservicefoundation.resourceprocessor;

import com.epam.training.microservicefoundation.resourceprocessor.client.ResourceServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.client.SongServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.KafkaTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.KafkaTopicTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.ResourceServiceClientTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.RetryTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.Server;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.SongServiceClientTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.WebClientTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.domain.ResourceRecord;
import com.epam.training.microservicefoundation.resourceprocessor.domain.ResourceType;
import com.epam.training.microservicefoundation.resourceprocessor.domain.SongRecordId;
import com.epam.training.microservicefoundation.resourceprocessor.service.ResourceProcessorService;
import com.epam.training.microservicefoundation.resourceprocessor.service.ResourceRecordValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import kotlin.jvm.functions.Function1;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import okio.Okio;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = {
        SpringExtension.class,
        KafkaExtension.class,
        MockitoExtension.class,
        ExternalServerExtension.class
})
@ContextConfiguration(classes = {
        KafkaTestConfiguration.class,
        KafkaTopicTestConfiguration.class,
        WebClientTestConfiguration.class,
        RetryTestConfiguration.class,
        ResourceServiceClientTestConfiguration.class,
        SongServiceClientTestConfiguration.class
})
@TestPropertySource(locations = "classpath:application.properties")
class KafkaManagerTest {
    private static final String TOPIC = "kafka.topic.resources";
    @Autowired
    private FakeKafkaProducer producer;
    @Autowired
    private ConsumerFactory<String, ResourceRecord> consumerFactory;
    @Autowired
    private Environment environment;
    private ResourceProcessorService resourceProcessorService;
    @Autowired
    private ResourceServiceClient resourceServiceClient;
    @Autowired
    private SongServiceClient songServiceClient;
    private KafkaConsumer<String, ResourceRecord> consumer;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setup() {
        consumer =(KafkaConsumer<String, ResourceRecord>) consumerFactory.createConsumer();
        resourceProcessorService = new ResourceProcessorService(new ResourceRecordValidator(), resourceServiceClient,
                songServiceClient);
        consumer.subscribe(Collections.singletonList(environment.getProperty(TOPIC)));
        consumer.poll(Duration.ofSeconds(5));

    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void shouldProduceAndConsumeRecordSuccessfully(@Server(serveTo = ResourceServiceClient.class)
                                                   MockWebServer resourceServiceServer,
                                                   @Server(serveTo = SongServiceClient.class)
                                                   MockWebServer songServiceServer) throws IOException,
            ExecutionException, InterruptedException {

        ResourceRecord resourceRecord = new ResourceRecord(1L);
        resourceServiceServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
                .setHeader(HttpHeaders.CONTENT_TYPE, ResourceType.MP3.getMimeType()).setBody(fileBuffer()));

        songServiceServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .setBody(mapper.writeValueAsString(new SongRecordId(1L))));

        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer, environment.getProperty(TOPIC));
        boolean isProcessed = resourceProcessorService.processResource(record.value());
        assertTrue(isProcessed);
    }

    @Test
    void shouldThrowExceptionWhenProduceAndConsumeRecord(@Server(serveTo = ResourceServiceClient.class)
                                             MockWebServer resourceServiceServer,
                                             @Server(serveTo = SongServiceClient.class)
                                             MockWebServer songServiceServer) throws ExecutionException,
            InterruptedException, IOException {

        ResourceRecord resourceRecord = new ResourceRecord(0L);
        resourceServiceServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
                .setHeader(HttpHeaders.CONTENT_TYPE, ResourceType.MP3.getMimeType()).setBody(fileBuffer()));

        songServiceServer.enqueue(new MockResponse().setResponseCode(HttpStatus.CREATED.value())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .setBody(mapper.writeValueAsString(new SongRecordId(1L))));

        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer, environment.getProperty(TOPIC));
        assertThrows(IllegalArgumentException.class, ()-> resourceProcessorService.processResource(record.value()));
    }
    
    @Test
    void shouldThrowExceptionWhenProduceAndConsumeWithWrongFile(@Server(serveTo = ResourceServiceClient.class)
                    MockWebServer resourceServiceServer) throws IOException, ExecutionException, InterruptedException {
        
        ResourceRecord resourceRecord = new ResourceRecord(1L);
        Buffer buffer = Okio.buffer(Okio.source(ResourceUtils.getFile("src/test/resources/files/wrong-audio.txt")))
                .getBuffer();

        resourceServiceServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
                .setHeader(HttpHeaders.CONTENT_TYPE, ResourceType.MP3.getMimeType()).setBody(buffer));

        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer, environment.getProperty(TOPIC));
        assertThrows(IllegalArgumentException.class, ()-> resourceProcessorService.processResource(record.value()));
    }

    @Test
    void shouldFailWhenProduceAndConsumeWithWrongSongServiceResult(@Server(serveTo = ResourceServiceClient.class)
                                                         MockWebServer resourceServiceServer,
                                                         @Server(serveTo = SongServiceClient.class)
                                                         MockWebServer songServiceServer) throws ExecutionException,
            InterruptedException, IOException {

        ResourceRecord resourceRecord = new ResourceRecord(1L);
        resourceServiceServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
                .setHeader(HttpHeaders.CONTENT_TYPE, ResourceType.MP3.getMimeType()).setBody(fileBuffer()));

        songServiceServer.enqueue(new MockResponse().setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR.value())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .setBody("Test"));

        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer, environment.getProperty(TOPIC));
        boolean isProcessed = resourceProcessorService.processResource(record.value());
        assertFalse(isProcessed);
    }

    @Test
    void shouldThrowExceptionWhenProduceAndConsumeWithWrongResourceServiceResult(@Server(serveTo =
            ResourceServiceClient.class) MockWebServer resourceServiceServer)
            throws ExecutionException, InterruptedException, IOException {

        ResourceRecord resourceRecord = new ResourceRecord(1L);
        resourceServiceServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE).setBody("Test"));


        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer, environment.getProperty(TOPIC));
        assertThrows(IllegalArgumentException.class, ()-> resourceProcessorService.processResource(record.value()));
    }

    @Test
    void shouldThrowExceptionWhenProduceAndConsumeRecord() throws ExecutionException, InterruptedException {
        producer.publish(new Object());
        assertThrows(IllegalStateException.class,() -> KafkaTestUtils.getSingleRecord(consumer, environment.getProperty(
                "kafka.topic.resources")));
    }

    private Buffer fileBuffer() throws IOException {
        File file = testFile();
        Buffer buffer = Okio.buffer(Okio.source(file)).getBuffer();
        Okio.use(buffer, (Function1<Buffer, Object>) buffer1 -> {
            try {
                return buffer.writeAll(Okio.source(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return buffer;
    }

    private File testFile() throws IOException {
        File file = ResourceUtils.getFile("src/test/resources/files/mpthreetest.mp3");
        File testFile = ResourceUtils.getFile("src/test/resources/files/test.mp3");
        if(!testFile.exists()) {
            Files.copy(file.toPath(), testFile.toPath());
        }
        return testFile;
    }
}


