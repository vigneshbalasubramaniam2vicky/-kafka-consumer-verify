package com.example.consumer.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "kafka_log")
public class ConsumerLog {
    @Id
    private String id;
    private String userId;
    private String name;
    private String email;
    private String phoneNumber;
    private LocalDateTime createdDt;
    private LocalDateTime updatedDt;
    private LocalDateTime kafkaReceivedDt;
    private String topicName;
}
