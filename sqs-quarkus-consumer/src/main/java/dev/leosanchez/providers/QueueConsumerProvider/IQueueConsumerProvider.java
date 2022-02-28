
package dev.leosanchez.providers.QueueConsumerProvider;

import java.util.List;
import dev.leosanchez.DTO.QueueMessage;

public interface IQueueConsumerProvider {
     public List<QueueMessage> pollMessages(String queueUrl, int maxNumberOfMessages);
     public void sendAnswer(String sourceQueueUrl, String responseMessage, String signature);
}
