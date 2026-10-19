package com.acme.payments.model;

import java.time.Instant;
import java.util.UUID;

/**
 * The event that flows through Kafka — this is what each "box" in the
 * diagram partitions represent (101, 102, 103... are offsets of these events).
 */
public class TransactionEvent {

    public enum Status {
        INITIATED, FRAUD_CHECKED, PROCESSED, NOTIFIED, RECORDED
    }

    public enum TransactionType {
        PAYMENT, TRANSFER, REFUND, WITHDRAWAL
    }

    private String transactionId;   // unique ID, used as Kafka message key
    private String userId;          // partition key — same user → same partition
    private double amount;
    private String currency;
    private TransactionType type;
    private Status status;
    private String description;
    private Instant timestamp;
    private String sourceAccount;
    private String destinationAccount;

    // Default constructor required by Jackson
    public TransactionEvent() {}

    public TransactionEvent(String userId, double amount, String currency,
                            TransactionType type, String description,
                            String sourceAccount, String destinationAccount) {
        this.transactionId = UUID.randomUUID().toString();
        this.userId = userId;
        this.amount = amount;
        this.currency = currency;
        this.type = type;
        this.status = Status.INITIATED;
        this.description = description;
        this.sourceAccount = sourceAccount;
        this.destinationAccount = destinationAccount;
        this.timestamp = Instant.now();
    }

    // Getters and setters
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public TransactionType getType() { return type; }
    public void setType(TransactionType type) { this.type = type; }

    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public String getSourceAccount() { return sourceAccount; }
    public void setSourceAccount(String sourceAccount) { this.sourceAccount = sourceAccount; }

    public String getDestinationAccount() { return destinationAccount; }
    public void setDestinationAccount(String destinationAccount) { this.destinationAccount = destinationAccount; }

    @Override
    public String toString() {
        return "TransactionEvent{id=" + transactionId + ", user=" + userId +
               ", amount=" + amount + " " + currency + ", status=" + status + "}";
    }
}
