package com.example.consumer.kafka.dlt;

import com.example.consumer.service.DltLogService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaDltConsumer {

    private final DltLogService dltLogService;

    @KafkaListener(topics = "${app.kafka.dlt-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeDlt(@Payload String failedMessage,
                           @Header(value = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
                           @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String errorMessage,
                           @Header(value = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer retryCount) {

        String resolvedTopic = originalTopic != null ? originalTopic : "unknown";
        String resolvedError = errorMessage != null ? errorMessage : "No error message available";
        Integer resolvedRetryCount = retryCount != null ? retryCount : 0;

        log.error("Consumed DLT message. originalTopic={}, retryCount={}, error={}",
                resolvedTopic, resolvedRetryCount, resolvedError);

        dltLogService.persistFailedMessage(resolvedTopic, failedMessage, resolvedError, resolvedRetryCount);
    }
}
