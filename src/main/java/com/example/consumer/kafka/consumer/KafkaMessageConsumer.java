package com.example.consumer.kafka.consumer;

import com.example.consumer.dto.ConsumerMessageDto;
import com.example.consumer.exception.MessageProcessingException;
import com.example.consumer.service.ConsumerLogService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaMessageConsumer {

    private final ConsumerLogService consumerLogService;

    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerMessageDto message,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {
        try {
            log.info("Received message. topic={}, partition={}, offset={}, userId={}", topic, partition, offset, message.getUserId());
            consumerLogService.persistConsumedMessage(message, topic);
            log.info("Message processed successfully. topic={}, offset={}, userId={}", topic, offset, message.getUserId());
        } catch (Exception ex) {
            log.error("Message processing failed. topic={}, offset={}, userId={}, error={}",
                    topic, offset, message != null ? message.getUserId() : null, ex.getMessage(), ex);
            throw new MessageProcessingException("Failed to process consumed message", ex);
        }
    }
}
