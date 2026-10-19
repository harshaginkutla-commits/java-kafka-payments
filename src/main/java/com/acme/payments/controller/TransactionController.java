package com.acme.payments.controller;

import com.acme.payments.consumer.AnalyticsConsumer;
import com.acme.payments.model.TransactionEvent;
import com.acme.payments.producer.TransactionProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * REST API — entry point for the diagram's left side (Mobile App, Web App, etc.)
 *
 * POST /api/transactions      — submit a payment (triggers the whole Kafka flow)
 * POST /api/transactions/demo — auto-generate sample transactions for learning
 * GET  /api/analytics         — live stats from AnalyticsConsumer
 */
@RestController
@RequestMapping("/api")
public class TransactionController {

    private final TransactionProducer producer;
    private final AnalyticsConsumer analytics;
    private final Random random = new Random();

    // Sample users — each will consistently go to same partition (by userId hash)
    private static final List<String> SAMPLE_USERS = List.of(
        "user-alice", "user-bob", "user-charlie", "user-diana", "user-eve"
    );

    public TransactionController(TransactionProducer producer, AnalyticsConsumer analytics) {
        this.producer = producer;
        this.analytics = analytics;
    }

    /**
     * Submit a payment transaction.
     * This is the "User adds items in app & clicks pay" step in the diagram.
     */
    @PostMapping("/transactions")
    public ResponseEntity<Map<String, Object>> submitTransaction(
            @RequestBody TransactionEvent event) {

        producer.publish(event);

        return ResponseEntity.accepted().body(Map.of(
            "transactionId", event.getTransactionId(),
            "status", "ACCEPTED",
            "message", "Transaction published to Kafka. Processing in real-time."
        ));
    }

    /**
     * Generate demo transactions to see Kafka in action.
     * Watch your logs — you'll see all 4 consumers fire for each transaction.
     *
     * @param count number of transactions to generate (default 5)
     */
    @PostMapping("/transactions/demo")
    public ResponseEntity<Map<String, Object>> generateDemoTransactions(
            @RequestParam(defaultValue = "5") int count) {

        for (int i = 0; i < count; i++) {
            TransactionEvent event = randomTransaction();
            producer.publish(event);

            // Small delay so you can see events flowing one by one in logs
            try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        return ResponseEntity.accepted().body(Map.of(
            "published", count,
            "message", "Watch your logs! Each transaction flows through all 4 consumers."
        ));
    }

    /**
     * Live analytics — powered by AnalyticsConsumer's in-memory aggregation.
     */
    @GetMapping("/analytics")
    public ResponseEntity<Map<String, Object>> getAnalytics() {
        return ResponseEntity.ok(analytics.getStats());
    }

    // --- helpers ---

    private TransactionEvent randomTransaction() {
        String userId = SAMPLE_USERS.get(random.nextInt(SAMPLE_USERS.size()));
        double amount = 100 + random.nextDouble() * 9900; // $100 - $10,000
        TransactionEvent.TransactionType type = TransactionEvent.TransactionType.values()[
            random.nextInt(TransactionEvent.TransactionType.values().length)
        ];

        return new TransactionEvent(
            userId,
            Math.round(amount * 100.0) / 100.0,
            "USD",
            type,
            "Demo transaction from " + userId,
            userId + "-account",
            "merchant-account-" + (random.nextInt(5) + 1)
        );
    }
}
