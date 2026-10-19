package com.acme.payments;

import com.acme.payments.model.TransactionEvent;
import com.acme.payments.producer.TransactionProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test using EmbeddedKafka — no real Kafka needed to run tests.
 * This is how you test Kafka code in CI/CD pipelines.
 */
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 3,
    topics = {"transactions"},
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:9099",
        "port=9099"
    }
)
class TransactionProducerTest {

    @Autowired
    private TransactionProducer producer;

    @Test
    void shouldPublishTransactionToKafka() throws Exception {
        TransactionEvent event = new TransactionEvent(
            "test-user",
            500.0,
            "USD",
            TransactionEvent.TransactionType.PAYMENT,
            "Test payment",
            "test-account",
            "merchant-account"
        );

        var future = producer.publish(event);
        var result = future.get(5, TimeUnit.SECONDS);

        assertThat(result).isNotNull();
        assertThat(result.getRecordMetadata().topic()).isEqualTo("transactions");
        assertThat(result.getRecordMetadata().partition()).isGreaterThanOrEqualTo(0);
        System.out.println("✓ Published to partition: " + result.getRecordMetadata().partition()
            + " offset: " + result.getRecordMetadata().offset());
    }
}
