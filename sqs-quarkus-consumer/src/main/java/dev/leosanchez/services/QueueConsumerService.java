package dev.leosanchez.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import dev.leosanchez.common.dto.QueueMessage;
import org.jboss.logging.Logger;

import dev.leosanchez.common.adapters.queueadapter.IQueueAdapter;
import dev.leosanchez.common.exceptions.MessagePollingException;
import dev.leosanchez.common.exceptions.MessageRemovalException;
import dev.leosanchez.common.exceptions.MessageSendingException;

@ApplicationScoped
public class QueueConsumerService {

    private static final Logger LOG = Logger.getLogger(QueueConsumerService.class);

    @Inject
    IQueueAdapter queueAdapter;

    public List<QueueMessage> pollMessages(String queueUrl, int maxNumberOfMessages) throws MessagePollingException {
        List<QueueMessage> messages = queueAdapter.receiveMessages(queueUrl, maxNumberOfMessages);
        messages.forEach(message -> {
            LOG.info("Received message " + message.getMessage());
            // we delete the message
            try{
                queueAdapter.deleteMessage(queueUrl, message.getReceiptHandle());
            } catch (MessageRemovalException e) {
                throw new RuntimeException(e);
            }
        });
        return messages;
    }

    public void sendAnswer(String sourceQueueUrl, String responseMessage, String signature)  throws MessageSendingException{
        LOG.info("Sending message " + responseMessage);
        Map<String, String> attributes = new HashMap<>() {
            {
                put("Signature", signature);
            }
        };
        LOG.info("url" + sourceQueueUrl);
        queueAdapter.sendMessageWithAttributes(sourceQueueUrl, responseMessage, attributes);
    }

}
