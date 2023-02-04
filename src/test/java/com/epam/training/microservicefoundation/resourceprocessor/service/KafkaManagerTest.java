package com.epam.training.microservicefoundation.resourceprocessor.service;

import com.epam.training.microservicefoundation.resourceprocessor.client.ResourceServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.client.SongServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.common.FakeKafkaProducer;
import com.epam.training.microservicefoundation.resourceprocessor.common.KafkaExtension;
import com.epam.training.microservicefoundation.resourceprocessor.common.MockServer;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.KafkaTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.KafkaTopicTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.ResourceServiceClientTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.RetryTemplateTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.SongServiceClientTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.WebClientTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.model.ResourceRecord;
import com.epam.training.microservicefoundation.resourceprocessor.model.ResourceType;
import com.epam.training.microservicefoundation.resourceprocessor.model.SongRecord;
import kotlin.jvm.functions.Function1;
import okio.Buffer;
import okio.Okio;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.ResourceUtils;
import reactor.netty.http.client.HttpClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Extensions registered declaratively via @ExtendWith at the class level, method level,
// or parameter level will be executed in the order in which they are declared in the source code.
// https://junit.org/junit5/docs/current/user-guide/#extensions-registration-declarative:~:text=Extensions%20registered%20declaratively,that%20order.

@ExtendWith(value = {
        KafkaExtension.class,
        SpringExtension.class,
})
@ContextConfiguration(classes = {
        KafkaTestConfiguration.class,
        KafkaTopicTestConfiguration.class,
        WebClientTestConfiguration.class,
        RetryTemplateTestConfiguration.class,
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
    @Autowired
    private RetryTemplate retryTemplate;
    @Autowired
    private Map<String, String> resourceClientHeader;
    @Autowired
    private FileConvertor fileConvertor;
    @Autowired
    private Map<String, String> songServiceClientHeader;
    @Autowired
    private HttpClient httpClient;
    private ResourceProcessorService resourceProcessorService;
    private KafkaConsumer<String, ResourceRecord> consumer;
    private MockServer resourceServiceServer;
    private MockServer songServiceServer;

    @BeforeEach
    void setup() {
        resourceServiceServer = MockServer.newInstance(httpClient);
        songServiceServer = MockServer.newInstance(httpClient);
        ResourceServiceClient resourceServiceClient = new ResourceServiceClient(resourceClientHeader, fileConvertor,
                retryTemplate, resourceServiceServer.getWebClient());

        SongServiceClient songServiceClient = new SongServiceClient(songServiceClientHeader, retryTemplate,
                songServiceServer.getWebClient());

        consumer =(KafkaConsumer<String, ResourceRecord>) consumerFactory.createConsumer();
        resourceProcessorService = new ResourceProcessorService(new ResourceRecordValidator(), resourceServiceClient,
                songServiceClient);

        consumer.subscribe(Collections.singletonList(environment.getProperty(TOPIC)));
        consumer.poll(Duration.ofSeconds(5));

    }

    @AfterEach
    void tearDown() throws IOException {
        consumer.close();
        resourceServiceServer.dispose();
        songServiceServer.dispose();
    }

    @Test
    void shouldProduceAndConsumeRecordSuccessfully() throws IOException,
            ExecutionException, InterruptedException {

        ResourceRecord resourceRecord = new ResourceRecord(1L);
        resourceServiceServer.responseWithBuffer(HttpStatus.OK, fileBuffer(),
                Collections.singletonMap(HttpHeaders.CONTENT_TYPE, ResourceType.MP3.getMimeType()));

        songServiceServer.responseWithJson(HttpStatus.CREATED, new SongRecord(1L),
                Collections.singletonMap(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE));

        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer, environment.getProperty(TOPIC));
        boolean isProcessed = resourceProcessorService.processResource(record.value());
        assertTrue(isProcessed);
    }

    @Test
    void shouldThrowIllegalArgumentExceptionExceptionWhenProduceAndConsumeRecord() throws IOException, ExecutionException, InterruptedException {

        ResourceRecord resourceRecord = new ResourceRecord(0L);
        resourceServiceServer.responseWithBuffer(HttpStatus.OK, fileBuffer(),
                Collections.singletonMap(HttpHeaders.CONTENT_TYPE, ResourceType.MP3.getMimeType()));

        songServiceServer.responseWithJson(HttpStatus.CREATED, new SongRecord(1L),
                Collections.singletonMap(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE));

        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer, environment.getProperty(TOPIC));
        assertThrows(IllegalArgumentException.class, ()-> resourceProcessorService.processResource(record.value()));
    }

    @Test
    void shouldThrowIllegalArgumentExceptionWhenProduceAndConsumeWithWrongFile() throws IOException, ExecutionException,
            InterruptedException {

        ResourceRecord resourceRecord = new ResourceRecord(1L);
        Buffer buffer = Okio.buffer(Okio.source(ResourceUtils.getFile("src/test/resources/files/wrong-audio.txt")))
                .getBuffer();

        resourceServiceServer.responseWithBuffer(HttpStatus.OK, buffer,
                Collections.singletonMap(HttpHeaders.CONTENT_TYPE, ResourceType.MP3.getMimeType()));

        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer, environment.getProperty(TOPIC));
        assertThrows(IllegalArgumentException.class, ()-> resourceProcessorService.processResource(record.value()));
    }

    @Test
    void shouldFailWhenProduceAndConsumeWithWrongSongServiceResult() throws ExecutionException,
            InterruptedException, IOException {

        ResourceRecord resourceRecord = new ResourceRecord(1L);
        resourceServiceServer.responseWithBuffer(HttpStatus.OK, fileBuffer(),
                Collections.singletonMap(HttpHeaders.CONTENT_TYPE, ResourceType.MP3.getMimeType()));

        songServiceServer.responseWithJson(HttpStatus.INTERNAL_SERVER_ERROR, "InternalServerError",
                Collections.singletonMap(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE));

        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer, environment.getProperty(TOPIC));
        boolean isProcessed = resourceProcessorService.processResource(record.value());
        assertFalse(isProcessed);

    }

    @Test
    void shouldThrowExceptionWhenProduceAndConsumeWithWrongResourceServiceResult()
            throws ExecutionException, InterruptedException, IOException {

        ResourceRecord resourceRecord = new ResourceRecord(1L);
        resourceServiceServer.responseWithJson(HttpStatus.OK, "Wrong response body test",
                Collections.singletonMap(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE));


        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer,
                environment.getProperty(TOPIC));

        assertThrows(IllegalArgumentException.class, ()-> resourceProcessorService.processResource(record.value()));
    }

    @Test
    void shouldThrowIllegalStateExceptionWhenProduceAndConsumeRecord() throws ExecutionException, InterruptedException {
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
