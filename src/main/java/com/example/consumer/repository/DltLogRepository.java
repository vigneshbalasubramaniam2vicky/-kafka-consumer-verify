package com.example.consumer.repository;

import com.example.consumer.entity.DltLog;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface DltLogRepository extends MongoRepository<DltLog, String> {
}
