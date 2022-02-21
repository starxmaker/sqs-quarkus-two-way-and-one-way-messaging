
package dev.leosanchez.providers.QueueConsumerProvider;

import java.util.List;

import dev.leosanchez.DTO.PollingRequest;
import dev.leosanchez.DTO.QueueMessage;

public interface IQueueConsumerProvider {
     public void listen(PollingRequest pollingRequest);
     public List<QueueMessage> pollMessages(String queueUrl, int maxNumberOfMessagesPerPolling);
     public void sendAnswer(String sourceQueueUrl, String responseMessage, String signature);
}
