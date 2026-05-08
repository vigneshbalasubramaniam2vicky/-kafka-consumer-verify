package com.example.consumer.service.impl;

import com.example.consumer.entity.DltLog;
import com.example.consumer.repository.DltLogRepository;
import com.example.consumer.service.DltLogService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class DltLogServiceImpl implements DltLogService {

    private final DltLogRepository dltLogRepository;

    @Override
    public void persistFailedMessage(String originalTopic, String failedMessage, String errorMessage, Integer retryCount) {
        DltLog doc = DltLog.builder()
                .originalTopic(originalTopic)
                .failedMessage(failedMessage)
                .errorMessage(errorMessage)
                .failedAt(LocalDateTime.now())
                .retryCount(retryCount)
                .build();

        DltLog saved = dltLogRepository.save(doc);
        log.info("DLT message persisted to MongoDB. mongoId={}, originalTopic={}", saved.getId(), originalTopic);
    }
}
