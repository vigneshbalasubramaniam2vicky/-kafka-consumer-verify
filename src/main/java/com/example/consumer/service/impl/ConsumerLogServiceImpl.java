package com.example.consumer.service.impl;

import com.example.consumer.dto.ConsumerMessageDto;
import com.example.consumer.entity.ConsumerLog;
import com.example.consumer.repository.ConsumerLogRepository;
import com.example.consumer.service.ConsumerLogService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerLogServiceImpl implements ConsumerLogService {

    private final ConsumerLogRepository consumerLogRepository;

    @Override
    public void persistConsumedMessage(ConsumerMessageDto dto, String topicName) {
        ConsumerLog logDoc = ConsumerLog.builder()
                .userId(dto.getUserId())
                .name(dto.getName())
                .email(dto.getEmail())
                .phoneNumber(dto.getPhoneNumber())
                .createdDt(dto.getCreatedDt())
                .updatedDt(dto.getUpdatedDt())
                .kafkaReceivedDt(LocalDateTime.now())
                .topicName(topicName)
                .build();

        ConsumerLog saved = consumerLogRepository.save(logDoc);
        log.info("Message persisted to MongoDB. mongoId={}, userId={}, topic={}", saved.getId(), saved.getUserId(), topicName);
    }
}
