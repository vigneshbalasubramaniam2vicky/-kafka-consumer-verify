package com.example.consumer.repository;

import com.example.consumer.entity.ConsumerLog;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ConsumerLogRepository extends MongoRepository<ConsumerLog, String> {
}
