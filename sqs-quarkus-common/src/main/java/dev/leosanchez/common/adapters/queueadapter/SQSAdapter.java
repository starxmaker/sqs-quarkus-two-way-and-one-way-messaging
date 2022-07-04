package dev.leosanchez.common.adapters.queueadapter;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.jboss.logging.Logger;

import dev.leosanchez.common.dto.QueueMessage;
import dev.leosanchez.common.exceptions.MessagePollingException;
import dev.leosanchez.common.exceptions.MessageRemovalException;
import dev.leosanchez.common.exceptions.MessageSendingException;
import dev.leosanchez.common.exceptions.QueueCreationException;
import dev.leosanchez.common.exceptions.QueueRemovalException;
import dev.leosanchez.common.exceptions.QueueRetrievalException;
import io.quarkus.arc.lookup.LookupIfProperty;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
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
    public void sendMessage(String targetQueueUrl, String message) throws MessageSendingException {
        sendMessageWithAttributes(targetQueueUrl, message, new HashMap<>());
    }
    
    @Override
    public void sendMessageWithAttributes(String targetQueueUrl, String message, Map<String, String> attributes) throws MessageSendingException {
        LOG.info("SQS - Sending message " + message);
        try {
            // we assign the attributes to the message
            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>() {
                {
                    attributes.forEach((key, value) -> {
                        put(key, MessageAttributeValue.builder().dataType("String").stringValue(value).build());
                    });
                }
            };
            // we build the request
            SendMessageRequest requestWithResponseUrl = SendMessageRequest.builder()
                    .queueUrl(targetQueueUrl)
                    .messageBody(message)
                    .messageAttributes(messageAttributes)
                    .build();
            // we send the request
            sqs.sendMessage(requestWithResponseUrl);
        } catch(Exception e) {
            LOG.error("SQS - Error sending message " + message, e);
            throw new MessageSendingException(e.getMessage());
        }
    }
    
    @Override
    public void deleteMessage(String queueUrl, String receiptHandle) throws MessageRemovalException {
        try {
            LOG.info("SQS - Deleting message with receipt handle: " + receiptHandle);
            sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(receiptHandle).build());
        } catch (Exception e) {
            LOG.error("SQS - Error deleting message with receipt handle: " + receiptHandle, e);
            throw new  MessageRemovalException(e.getMessage());
        }
    }

    @Override
    public String createQueue(String queueName) throws QueueCreationException {
        try {
            LOG.info("SQS - Creating queue: " + queueName);
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            // we return the created queue url
            return sqs.createQueue(createQueueRequest).queueUrl();
        } catch (Exception e) {
            LOG.error("SQS - Error creating queue: " + queueName, e);
            throw new QueueCreationException(e.getMessage());
        }
    }

    @Override
    public void deleteQueue(String queueUrl) throws QueueRemovalException {
        try {
            LOG.info("SQS - Deleting queue: " + queueUrl);
            DeleteQueueRequest request = DeleteQueueRequest.builder().queueUrl(queueUrl).build();
            sqs.deleteQueue(request);
        } catch (Exception e) {
            LOG.error("SQS - Error while deleting queue", e);
            throw new QueueRemovalException(e.getMessage());
        }
    }


    @Override
    public Optional<String> getQueueUrl(String queueName) throws QueueRetrievalException {
        try {
            LOG.info("SQS - Checking queue existence: " + queueName);
            GetQueueUrlRequest request = GetQueueUrlRequest.builder().queueName(queueName).build();
            GetQueueUrlResponse response = sqs.getQueueUrl(request);
            return Optional.of(response.queueUrl());
        } catch (QueueDoesNotExistException e) {
            return Optional.empty();
        } catch (Exception e) {
            throw new QueueRetrievalException(e.getMessage());
        }
    }
}