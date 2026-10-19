package com.acme.payments.consumer;

import com.acme.payments.model.TransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * CONSUMER 2: Payment Processing Service — diagram shows this as main processor.
 *
 * This is the core business logic consumer.
 * Simulates debiting source account and crediting destination account.
 *
 * Key Kafka concept demonstrated here: consumer group "payment-processing-group"
 * means only ONE instance of this service processes each message
 * (unlike fraud detection which uses its own independent group).
 */
@Service
public class PaymentProcessingConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessingConsumer.class);

    @KafkaListener(
        topics = "${kafka.topic.transactions}",
        groupId = "payment-processing-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, TransactionEvent> record) {
        TransactionEvent event = record.value();

        log.info("[PAYMENT] Processing transactionId={} type={} amount={} {}",
                event.getTransactionId(), event.getType(),
                event.getAmount(), event.getCurrency());

        // Simulate processing time (real: DB write, external payment gateway call)
        simulateProcessing();

        event.setStatus(TransactionEvent.Status.PROCESSED);

        log.info("✓ [PAYMENT] PROCESSED transactionId={} | {} → {} | amount={} {}",
                event.getTransactionId(),
                event.getSourceAccount(),
                event.getDestinationAccount(),
                event.getAmount(),
                event.getCurrency());
    }

    private void simulateProcessing() {
        try {
            Thread.sleep(100); // simulate 100ms processing
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
