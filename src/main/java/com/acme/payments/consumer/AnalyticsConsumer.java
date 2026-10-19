package com.acme.payments.consumer;

import com.acme.payments.model.TransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CONSUMER 4: Analytics/Reporting — bottom of diagram's consumer list.
 * Also feeds Data Lake/Warehouse (for BI & ML).
 *
 * Keeps running totals in memory (real system: writes to ClickHouse, BigQuery, etc.)
 * This consumer demonstrates that Kafka enables the SAME events to feed
 * both operational systems (payment processing) and analytical systems simultaneously.
 */
@Service
public class AnalyticsConsumer {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsConsumer.class);

    // In-memory analytics store (real: time-series DB or data warehouse)
    private final AtomicLong totalTransactions = new AtomicLong(0);
    private final Map<String, Double> totalAmountByType = new ConcurrentHashMap<>();
    private final Map<Integer, Long> countByPartition = new ConcurrentHashMap<>();

    @KafkaListener(
        topics = "${kafka.topic.transactions}",
        groupId = "analytics-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, TransactionEvent> record) {
        TransactionEvent event = record.value();

        // Track which partition this came from (educational: see how userId distributes)
        countByPartition.merge(record.partition(), 1L, Long::sum);

        // Aggregate metrics
        long total = totalTransactions.incrementAndGet();
        totalAmountByType.merge(
            event.getType().name(),
            event.getAmount(),
            Double::sum
        );

        log.info("[ANALYTICS] partition={} | Total txns={} | Type={} cumulative={}",
                record.partition(), total,
                event.getType(),
                totalAmountByType.get(event.getType().name()));

        // Log partition distribution every 5 transactions
        if (total % 5 == 0) {
            log.info("[ANALYTICS] Partition distribution: {}", countByPartition);
            log.info("[ANALYTICS] Volume by type: {}", totalAmountByType);
        }
    }

    // Exposed via REST endpoint for the dashboard
    public Map<String, Object> getStats() {
        return Map.of(
            "totalTransactions", totalTransactions.get(),
            "amountByType", totalAmountByType,
            "countByPartition", countByPartition
        );
    }
}
