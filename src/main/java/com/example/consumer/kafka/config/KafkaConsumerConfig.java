package com.example.consumer.kafka.config;

import com.example.consumer.config.KafkaTopicProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;


@Slf4j
@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicProperties kafkaTopicProperties;

    /**
     * Main topic error handler:
     * - Retries message processing with exponential backoff.
     * - After retries are exhausted, publishes to DLT.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("Retries exhausted. Publishing to DLT. topic={}, partition={}, offset={}, error={}",
                            record.topic(), record.partition(), record.offset(), ex.getMessage());
                    return new TopicPartition(kafkaTopicProperties.getDltTopic(), record.partition());
                }
        );

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries((int) kafkaTopicProperties.getRetryAttempts());
        backOff.setInitialInterval(kafkaTopicProperties.getRetryIntervalMs());
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(kafkaTopicProperties.getMaxRetryIntervalMs());

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);

        errorHandler.setRetryListeners((ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) ->
                log.warn("Retry attempt {} for topic={}, partition={}, offset={}, cause={}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage()));

        return errorHandler;
    }

    /**
     * DLT error handler:
     * - No retries.
     * - No re-publish to DLT (prevents infinite loop).
     */
    @Bean
    public CommonErrorHandler dltErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0L, 0L));
        errorHandler.setRetryListeners((record, ex, attempt) ->
                log.error("DLT processing failed with no retry. topic={}, partition={}, offset={}, error={}",
                        record.topic(), record.partition(), record.offset(), ex.getMessage(), ex));
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);

        factory.setCommonErrorHandler(kafkaErrorHandler);
        factory.getContainerProperties().setObservationEnabled(true);

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> dltKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<String, Object> consumerFactory,
            CommonErrorHandler dltErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);

        factory.setCommonErrorHandler(dltErrorHandler);
        factory.setConcurrency(1);
        factory.getContainerProperties().setObservationEnabled(true);

        return factory;
    }

}
