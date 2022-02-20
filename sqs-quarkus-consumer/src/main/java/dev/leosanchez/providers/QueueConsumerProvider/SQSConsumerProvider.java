package dev.leosanchez.providers.QueueConsumerProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
@LookupIfProperty(name = "queueconsumer.provider", stringValue = "sqs")
public class SQSConsumerProvider implements IQueueConsumerProvider {

    private static final Logger LOG = Logger.getLogger(SQSConsumerProvider.class);

    @Inject
    SqsClient sqs;

    @Override
    public List<QueueMessage> pollMessages(String queueUrl) {
        // we send a request to poll the messages for the following 20 seconds
        List<QueueAttributeName> queueAttributeNames = List.of(QueueAttributeName.ALL);
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)
                .messageAttributeNames("All")
                .attributeNames(queueAttributeNames)
                .build();
        // we get the response
        List<Message> rawMessages = sqs.receiveMessage(request).messages();
        // we transform the raw messages into our object
        List<QueueMessage> messages = new ArrayList<>();
        for (Message rawMessage : rawMessages) {
            messages.add(new QueueMessage(rawMessage.body(), rawMessage.messageAttributes().get("ResponseQueueUrl").stringValue(), rawMessage.messageAttributes().get("Signature").stringValue() ));
            // we remove the message from the original queue
            deleteMessage(queueUrl, rawMessage.receiptHandle());
        }
        return messages;
    }

    @Override
    public void sendAnswer(String sourceQueueUrl, String responseMessage, String signature) {
        LOG.info("Sending message " + responseMessage);
        // we assign the attributes to the message
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>() {
            {
                // we attach the original signature to the message
                put("Signature", MessageAttributeValue.builder().dataType("String").stringValue(signature).build());
            }
        };
        // we build the request
        SendMessageRequest requestWithResponseUrl = SendMessageRequest.builder()
                .queueUrl(sourceQueueUrl)
                .messageBody(responseMessage)
                .messageAttributes(messageAttributes)
                .build();
        // we send the request
        sqs.sendMessage(requestWithResponseUrl);
        
    }

    private void deleteMessage(String queueUrl, String receiptHandle) {
        LOG.info("Deleting message");
        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(receiptHandle).build());
    }
}
