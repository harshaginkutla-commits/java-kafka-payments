package com.acme.payments.consumer;

import com.acme.payments.model.TransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * CONSUMER 1: Fraud Detection Service — top of diagram's consumer list.
 *
 * Reads every transaction event and flags suspicious ones.
 * Runs in parallel with other consumers (same topic, different consumer group).
 *
 * Real-world: would call an ML model or rules engine.
 * Here: simple rule — flag transactions over $10,000.
 */
@Service
public class FraudDetectionConsumer {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionConsumer.class);
    private static final double FRAUD_THRESHOLD = 10_000.0;

    @KafkaListener(
        topics = "${kafka.topic.transactions}",
        groupId = "fraud-detection-group",          // separate group = independent consumer
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, TransactionEvent> record) {
        TransactionEvent event = record.value();

        log.info("[FRAUD-CHECK] partition={} offset={} transactionId={} amount={}",
                record.partition(), record.offset(),
                event.getTransactionId(), event.getAmount());

        // Simple fraud rule: flag large transactions
        if (event.getAmount() > FRAUD_THRESHOLD) {
            log.warn("🚨 [FRAUD-CHECK] SUSPICIOUS transaction detected! transactionId={} amount={}",
                    event.getTransactionId(), event.getAmount());
            // In real system: publish to fraud-alerts topic, block transaction, notify compliance team
        } else {
            log.info("✓ [FRAUD-CHECK] Transaction CLEARED transactionId={}", event.getTransactionId());
        }
    }
}
