package com.example.consumer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaTopicProperties {
    private String topic;
    private String dltTopic;
    private long retryAttempts;
    private long retryIntervalMs;
}
