package com.acme.payments.producer;

import com.acme.payments.model.TransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * PRODUCER — left side of the diagram.
 * Receives a payment event and publishes it to the "transactions" Kafka topic.
 *
 * Key insight: partitioned by userId, so all events from the same user
 * always go to the same partition → ordered processing per user.
 */
@Service
public class TransactionProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducer.class);

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    @Value("${kafka.topic.transactions}")
    private String topic;

    public TransactionProducer(KafkaTemplate<String, TransactionEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publish a transaction event to Kafka.
     *
     * @param event the payment event
     * The KEY is userId — Kafka uses this to decide which partition to write to.
     * Same userId → same partition → guaranteed ordering for that user.
     */
    public CompletableFuture<SendResult<String, TransactionEvent>> publish(TransactionEvent event) {
        String partitionKey = event.getUserId();  // partition key = userId (like diagram: choose right partition key)

        log.info("→ PUBLISHING to topic={}, partition-key={}, transactionId={}",
                topic, partitionKey, event.getTransactionId());

        CompletableFuture<SendResult<String, TransactionEvent>> future =
                kafkaTemplate.send(topic, partitionKey, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("✓ Published transactionId={} to partition={} offset={}",
                        event.getTransactionId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("✗ Failed to publish transactionId={}: {}",
                        event.getTransactionId(), ex.getMessage());
            }
        });

        return future;
    }
}
