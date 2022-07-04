package dev.leosanchez.common.adapters.queueadapter;

import dev.leosanchez.common.dto.QueueMessage;
import dev.leosanchez.common.exceptions.MessagePollingException;
import dev.leosanchez.common.exceptions.MessageRemovalException;
import dev.leosanchez.common.exceptions.MessageSendingException;
import dev.leosanchez.common.exceptions.QueueCreationException;
import dev.leosanchez.common.exceptions.QueueRemovalException;
import dev.leosanchez.common.exceptions.QueueRetrievalException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface IQueueAdapter {
    public void sendMessage(String targetQueueUrl, String message) throws MessageSendingException;
    public void sendMessageWithAttributes(String targetQueueUrl, String message, Map<String, String> attributes) throws MessageSendingException;
    public List<QueueMessage> receiveMessages(String queueUrl, Integer maxNumberOfMessages) throws MessagePollingException;
    public void deleteMessage(String queueUrl, String receiptHandle) throws MessageRemovalException;
    public String createQueue(String queueName) throws QueueCreationException;
    public void deleteQueue(String queueUrl) throws QueueRemovalException;
    public Optional<String> getQueueUrl(String queueName) throws QueueRetrievalException;
}