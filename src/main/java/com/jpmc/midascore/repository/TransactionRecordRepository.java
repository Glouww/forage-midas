package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.TransactionRecord;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface TransactionRecordRepository extends CrudRepository<TransactionRecord, Long> {
    List<TransactionRecord> findBySenderName(String senderName);
    List<TransactionRecord> findByRecipientName(String recipientName);
    List<TransactionRecord> findBySenderNameOrRecipientName(String senderName, String recipientName);
}

