package com.example.consumer.service;

public interface DltLogService {
    void persistFailedMessage(String originalTopic, String failedMessage, String errorMessage, Integer retryCount);
}
