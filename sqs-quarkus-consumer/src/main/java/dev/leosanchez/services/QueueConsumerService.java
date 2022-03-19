package dev.leosanchez.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.jboss.logging.Logger;

import dev.leosanchez.DTO.QueueMessage;
import dev.leosanchez.adapters.QueueAdapter.IQueueAdapter;

@ApplicationScoped
public class QueueConsumerService {

    private static final Logger LOG = Logger.getLogger(QueueConsumerService.class);

    @Inject
    IQueueAdapter queueAdapter;

    public List<QueueMessage> pollMessages(String queueUrl, int maxNumberOfMessages) {
        List<QueueMessage> messages = queueAdapter.receiveMessages(queueUrl, maxNumberOfMessages);
        messages.forEach(message -> {
            LOG.info("Received message " + message.getMessage());
            // we delete the message
            queueAdapter.deleteMessage(queueUrl, message.getReceiptHandle());
        });
        return messages;
    }

    public void sendAnswer(String sourceQueueUrl, String responseMessage, String signature) {
        LOG.info("Sending message " + responseMessage);
        Map<String, String> attributes = new HashMap<>() {
            {
                put("Signature", signature);
            }
        };
        queueAdapter.sendMessage(sourceQueueUrl, responseMessage, attributes);
    }

}
