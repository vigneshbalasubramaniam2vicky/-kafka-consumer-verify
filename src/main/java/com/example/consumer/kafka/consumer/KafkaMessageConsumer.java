package com.example.consumer.kafka.consumer;

import com.example.consumer.dto.ConsumerMessageDto;
import com.example.consumer.exception.MessageProcessingException;
import com.example.consumer.service.ConsumerLogService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaMessageConsumer {

    private final ConsumerLogService consumerLogService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(String rawMessage,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {
        try {
            ConsumerMessageDto message = convertToDto(rawMessage);
            log.info("Received message. topic={}, partition={}, offset={}, userId={}",
                    topic, partition, offset, message.getUserId());
            consumerLogService.persistConsumedMessage(message, topic);
            log.info("Message processed successfully. topic={}, offset={}, userId={}",
                    topic, offset, message.getUserId());
        } catch (Exception ex) {
            log.error("Message processing failed. topic={}, partition={}, offset={}, payload={}, error={}",
                    topic, partition, offset, rawMessage, ex.getMessage(), ex);
            throw new MessageProcessingException("Failed to process consumed message", ex);
        }
    }

    private ConsumerMessageDto convertToDto(String rawMessage) throws JsonProcessingException {
        if (rawMessage == null || rawMessage.isBlank()) {
            throw new IllegalArgumentException("Message payload is null/blank");
        }

        String normalized = rawMessage.trim();
        if (!looksLikeJson(normalized)) {
            normalized = maybeDecodeBase64(normalized);
        }

        if (!looksLikeJson(normalized)) {
            throw new IllegalArgumentException("Payload is neither JSON nor base64-encoded JSON");
        }

        return objectMapper.readValue(normalized, ConsumerMessageDto.class);
    }

    private boolean looksLikeJson(String payload) {
        return payload.startsWith("{") && payload.endsWith("}");
    }

    private String maybeDecodeBase64(String payload) {
        try {
            byte[] decoded = Base64.getDecoder().decode(payload);
            return new String(decoded, StandardCharsets.UTF_8).trim();
        } catch (IllegalArgumentException ignored) {
            return payload;
        }
    }
}
