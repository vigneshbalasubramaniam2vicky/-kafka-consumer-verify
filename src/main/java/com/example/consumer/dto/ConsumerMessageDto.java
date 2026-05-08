package com.example.consumer.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerMessageDto {
    private String id;
    private String userId;
    private String name;
    private String email;
    private String phoneNumber;
    private LocalDateTime createdDt;
    private LocalDateTime updatedDt;
}
