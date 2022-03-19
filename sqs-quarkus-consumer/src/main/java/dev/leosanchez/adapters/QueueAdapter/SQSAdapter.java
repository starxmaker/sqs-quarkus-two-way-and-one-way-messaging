package dev.leosanchez.adapters.QueueAdapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.jboss.logging.Logger;

import dev.leosanchez.DTO.QueueMessage;
import io.quarkus.arc.lookup.LookupIfProperty;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@ApplicationScoped
@LookupIfProperty(name = "queue.provider", stringValue = "sqs")
public class SQSAdapter implements IQueueAdapter {

    // just a logger
    private static final Logger LOG = Logger.getLogger(SQSAdapter.class);

    // the sdk client
    @Inject
    SqsClient sqs;

    @Override
    public void sendMessage(String targetQueueUrl, String message, Map<String, String> attributes) {
        LOG.info("SQS - Sending message " + message);
        // we assign the attributes to the message
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>() {
            {
                attributes.forEach((key, value) -> {
                    put(key, MessageAttributeValue.builder().dataType("String").stringValue(value).build());
                });
            }
        };
        // we build thSe request
        SendMessageRequest requestWithResponseUrl = SendMessageRequest.builder()
                .queueUrl(targetQueueUrl)
                .messageBody(message)
                .messageAttributes(messageAttributes)
                .build();
        // we send the request
        sqs.sendMessage(requestWithResponseUrl);
    }

    @Override
    public List<QueueMessage> receiveMessages(String queueUrl, Integer maxNumberPerMessages) {
        List<QueueMessage> response = new ArrayList<>();
        LOG.info("SQS - Polling messages");
        // we prepare the request
        List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxNumberPerMessages)
                .waitTimeSeconds(20) // long polling
                .messageAttributeNames("All")
                .attributeNames(List.of(QueueAttributeName.ALL))
                .build()).messages();
        if (messages.size() > 0) {
            LOG.info("SQS - Messages received");
            for (Message message : messages) {
                Map<String, String> attributes = new HashMap<>() {{
                    message.messageAttributes().forEach((key, value) -> {
                        put(key, value.stringValue());
                    });
                }};
                QueueMessage queueMessage = new QueueMessage(message.body(), message.receiptHandle(), attributes);
                response.add(queueMessage);
            }
        } else {
            LOG.info("SQS - No messages");
        }
        return response;
    }

    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) {
        LOG.info("SQS - Deleting message with receipt handle " + receiptHandle);
        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(receiptHandle).build());
    }

}
