package com.acme.payments.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Creates Kafka topics on startup.
 * Matches the diagram: TOPIC: TRANSACTIONS with 3 partitions (PARTITION 0, 1, 2)
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.transactions}")
    private String transactionsTopic;

    @Value("${kafka.topic.partitions}")
    private int partitions;

    @Value("${kafka.topic.replicas}")
    private int replicas;

    /**
     * Main topic — all payment events flow through here.
     * 3 partitions = 3 parallel streams of events (like diagram shows PARTITION 0, 1, 2)
     */
    @Bean
    public NewTopic transactionsTopic() {
        return TopicBuilder.name(transactionsTopic)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }

    /**
     * Dead Letter Topic — failed events land here instead of being lost.
     * Pro tip from diagram: enable idempotent producers + handle failures gracefully.
     */
    @Bean
    public NewTopic transactionsDltTopic() {
        return TopicBuilder.name(transactionsTopic + ".DLT")
                .partitions(1)
                .replicas(replicas)
                .build();
    }
}
