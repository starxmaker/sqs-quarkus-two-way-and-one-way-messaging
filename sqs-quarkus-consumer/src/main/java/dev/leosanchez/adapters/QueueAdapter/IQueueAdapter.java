
package dev.leosanchez.adapters.QueueAdapter;

import java.util.List;
import java.util.Map;

import dev.leosanchez.DTO.QueueMessage;

public interface IQueueAdapter {
     public List<QueueMessage> receiveMessages(String queueUrl, Integer maxNumberOfMessages);
     public void sendMessage(String targetQueueUrl, String message, Map<String, String> attributes);
     public void deleteMessage(String queueUrl, String receiptHandle);
}
