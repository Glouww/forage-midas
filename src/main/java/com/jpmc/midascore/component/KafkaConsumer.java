package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    
    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public KafkaConsumer(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "my-group")
    public void listen(Transaction transaction) {
        String amountStr = String.format("%.2f", transaction.getAmount());
        logger.info("Received Transaction: senderId={}, recipientId={}, amount=${}", 
                   transaction.getSenderId(), transaction.getRecipientId(), amountStr);
        
        // Get sender and recipient for validation
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        
        // Validate transaction
        if (!isValidTransaction(transaction, sender, recipient)) {
            logger.warn("Transaction validation failed - discarding transaction");
            return;
        }
        
        // Create and save transaction record
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
        transactionRecordRepository.save(transactionRecord);
        
        // Update balances
        float oldSenderBalance = sender.getBalance();
        float oldRecipientBalance = recipient.getBalance();
        sender.setBalance(oldSenderBalance - transaction.getAmount());
        recipient.setBalance(oldRecipientBalance + transaction.getAmount());
        
        // Save updated user balances
        userRepository.save(sender);
        userRepository.save(recipient);
        
        logger.info("Transaction processed successfully: '{}' sent ${} to '{}' | Sender balance: ${} → ${} | Recipient balance: ${} → ${}", 
                   sender.getName(), 
                   amountStr,
                   recipient.getName(),
                   String.format("%.2f", oldSenderBalance), 
                   String.format("%.2f", sender.getBalance()),
                   String.format("%.2f", oldRecipientBalance), 
                   String.format("%.2f", recipient.getBalance()));
    }
    
    private boolean isValidTransaction(Transaction transaction, UserRecord sender, UserRecord recipient) {
        // Check if senderId is valid
        if (sender == null) {
            logger.warn("Invalid senderId: {} - User not found in database", transaction.getSenderId());
            return false;
        }
        
        // Check if recipientId is valid
        if (recipient == null) {
            logger.warn("Invalid recipientId: {} - User not found in database", transaction.getRecipientId());
            return false;
        }
        
        // Check if sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance - Sender '{}' has ${} but needs ${}", 
                       sender.getName(), 
                       String.format("%.2f", sender.getBalance()), 
                       String.format("%.2f", transaction.getAmount()));
            return false;
        }
        
        return true;
    }
    
    /**
     * Helper method to find a user by name.
     * Uses findAll() since UserRepository doesn't have findByName method.
     * 
     * @param name The name of the user to find
     * @return The UserRecord if found, null otherwise
     */
    public UserRecord findUserByName(String name) {
        Iterable<UserRecord> allUsers = userRepository.findAll();
        for (UserRecord user : allUsers) {
            if (user.getName().equals(name)) {
                return user;
            }
        }
        return null;
    }
}

