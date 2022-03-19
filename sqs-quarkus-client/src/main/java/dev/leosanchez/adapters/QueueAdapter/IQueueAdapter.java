package dev.leosanchez.adapters.QueueClientAdapter;

import java.util.List;
import java.util.Map;

import dev.leosanchez.DTO.QueueMessage;

public interface IQueueClientAdapter {

    public void sendMessage(String targetQueueUrl, String message, Map<String, String> attributes);
    public List<QueueMessage> receiveMessages(String queueUrl, Integer maxNumberOfMessages);
    public void deleteMessage(String queueUrl, String receiptHandle);
    public String createQueue(String name);
    public void deleteQueue(String queueUrl);
    
}