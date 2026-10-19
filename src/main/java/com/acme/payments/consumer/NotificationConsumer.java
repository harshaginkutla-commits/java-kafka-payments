package com.acme.payments.consumer;

import com.acme.payments.model.TransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * CONSUMER 3: Notification Service (Email/SMS) — third in diagram.
 *
 * Sends confirmation to user after payment.
 * Runs independently — even if Payment Processing is slow, notifications
 * don't block and don't wait for it (that's the beauty of Kafka fan-out).
 *
 * Real-world: would call SendGrid, Twilio, Firebase, etc.
 * Here: logs the notification content.
 */
@Service
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    @KafkaListener(
        topics = "${kafka.topic.transactions}",
        groupId = "notification-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, TransactionEvent> record) {
        TransactionEvent event = record.value();

        log.info("[NOTIFY] Sending notification for transactionId={} to userId={}",
                event.getTransactionId(), event.getUserId());

        // Simulate email/SMS
        sendEmail(event);
        sendSms(event);

        log.info("✓ [NOTIFY] Notifications sent for transactionId={}", event.getTransactionId());
    }

    private void sendEmail(TransactionEvent event) {
        // Real: call SendGrid/SES API
        log.info("📧 [EMAIL] To: {}@example.com | Your {} of {} {} is confirmed. Ref: {}",
                event.getUserId(),
                event.getType().toString().toLowerCase(),
                event.getAmount(),
                event.getCurrency(),
                event.getTransactionId());
    }

    private void sendSms(TransactionEvent event) {
        // Real: call Twilio API
        log.info("📱 [SMS]   To: user={} | TXN {} {} {} CONFIRMED",
                event.getUserId(),
                event.getTransactionId().substring(0, 8),
                event.getAmount(),
                event.getCurrency());
    }
}
