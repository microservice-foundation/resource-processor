package com.epam.training.microservicefoundation.resourceprocessor;

import com.epam.training.microservicefoundation.resourceprocessor.client.ResourceServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.client.SongServiceClient;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.KafkaTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.configuration.KafkaTopicTestConfiguration;
import com.epam.training.microservicefoundation.resourceprocessor.domain.ResourceRecord;
import com.epam.training.microservicefoundation.resourceprocessor.domain.SongRecordId;
import com.epam.training.microservicefoundation.resourceprocessor.service.ResourceProcessorService;
import com.epam.training.microservicefoundation.resourceprocessor.service.ResourceRecordValidator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
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
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(value = {SpringExtension.class, KafkaExtension.class, MockitoExtension.class})
@ContextConfiguration(classes = {KafkaTestConfiguration.class, KafkaTopicTestConfiguration.class})
@TestPropertySource(locations = "classpath:application.yaml")
class KafkaManagerTest {

    @Autowired
    private FakeKafkaProducer producer;
    @Autowired
    private ConsumerFactory<String, ResourceRecord> consumerFactory;
    @Autowired
    private Environment environment;
    private ResourceProcessorService resourceProcessorService;
    @Mock
    private ResourceServiceClient resourceServiceClient;
    @Mock
    private SongServiceClient songServiceClient;
    private KafkaConsumer<String, ResourceRecord> consumer;


    @BeforeEach
    void setup() {
        consumer =(KafkaConsumer<String, ResourceRecord>) consumerFactory.createConsumer();
        resourceProcessorService = new ResourceProcessorService(new ResourceRecordValidator(), resourceServiceClient,
                songServiceClient);
        consumer.subscribe(Collections.singletonList(environment.getProperty("kafka.topic.resources")));
        consumer.poll(Duration.ofSeconds(5));

    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void shouldProduceAndConsumeRecordSuccessfully() throws IOException, ExecutionException, InterruptedException {
        ResourceRecord resourceRecord = new ResourceRecord(1L);
        File testFile = testFile();
        when(resourceServiceClient.getById(resourceRecord.getId())).thenReturn(Optional.of(testFile));
        when(songServiceClient.post(any())).thenReturn(new SongRecordId(1L));

        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer, environment.getProperty("kafka.topic.resources"));
        boolean isProcessed = resourceProcessorService.processResource(record.value());
        assertTrue(isProcessed);
    }

    @Test
    void shouldProduceAndConsumeRecordFailed() throws IOException, ExecutionException, InterruptedException {
        ResourceRecord resourceRecord = new ResourceRecord(1L);
        when(resourceServiceClient.getById(resourceRecord.getId())).thenReturn(Optional.empty());

        producer.publish(resourceRecord);
        ConsumerRecord<String, ResourceRecord> record = KafkaTestUtils.getSingleRecord(consumer, environment.getProperty("kafka.topic.resources"));
        boolean isProcessed = resourceProcessorService.processResource(record.value());
        assertFalse(isProcessed);
    }

    @Test
    void shouldThrowExceptionWhenProduceAndConsumeRecord() throws ExecutionException,
            InterruptedException {

        producer.publish(new Object());
        assertThrows(IllegalStateException.class,() -> KafkaTestUtils.getSingleRecord(consumer, environment.getProperty(
                "kafka.topic.resources")));
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


