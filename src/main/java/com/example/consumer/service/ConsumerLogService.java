package com.example.consumer.service;

import com.example.consumer.dto.ConsumerMessageDto;

public interface ConsumerLogService {
    void persistConsumedMessage(ConsumerMessageDto dto, String topicName);
}
