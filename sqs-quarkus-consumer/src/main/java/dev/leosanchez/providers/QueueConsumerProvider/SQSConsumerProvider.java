package dev.leosanchez.providers.QueueConsumerProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.jboss.logging.Logger;

import dev.leosanchez.DTO.PollingRequest;
import dev.leosanchez.DTO.QueueMessage;
import io.quarkus.arc.lookup.LookupIfProperty;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@ApplicationScoped
@LookupIfProperty(name = "queueconsumer.provider", stringValue = "sqs")
public class SQSConsumerProvider implements IQueueConsumerProvider {

    private static final Logger LOG = Logger.getLogger(SQSConsumerProvider.class);

    @Inject
    SqsClient sqs;

    @Override
    public List<QueueMessage> pollMessages(String queueUrl, int maxNumberOfMessagesPerPolling) {
        // we send a request to poll the messages for the following 20 seconds
        List<QueueAttributeName> queueAttributeNames = List.of(QueueAttributeName.ALL);
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxNumberOfMessagesPerPolling)
                .waitTimeSeconds(20)
                .messageAttributeNames("All")
                .attributeNames(queueAttributeNames)
                .build();
        // we get the response
        List<Message> rawMessages = sqs.receiveMessage(request).messages();
        // we transform the raw messages into our object
        List<QueueMessage> messages = new ArrayList<>();
        for (Message rawMessage : rawMessages) {
            messages.add(new QueueMessage(rawMessage.body(),
                    rawMessage.messageAttributes().get("ResponseQueueUrl").stringValue(),
                    rawMessage.messageAttributes().get("Signature").stringValue()));
            // we remove the message from the original queue
            deleteMessage(queueUrl, rawMessage.receiptHandle());
        }
        return messages;
    }

    @Override
    public void sendAnswer(String sourceQueueUrl, String responseMessage, String signature) {
        try {
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
        } catch (QueueDoesNotExistException e) {
            LOG.error("Queue does not exist so the message cannot be sent");
            e.printStackTrace();
        }

    }

    private void deleteMessage(String queueUrl, String receiptHandle) {
        LOG.info("Deleting message");
        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(receiptHandle).build());
    }

    @Override
    public void listen(PollingRequest pollingRequest) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                // we start listening
                while (!Thread.interrupted()) {
                    LOG.info("polling messages for queue " + pollingRequest.getQueueUrl());
                    // we poll messages from the queue
                    List<QueueMessage> messages = pollMessages(pollingRequest.getQueueUrl(),
                            pollingRequest.getMaxNumberOfMessagesPerPolling());
                    if (messages.isEmpty()) {
                        LOG.info("No messages received for queue" + pollingRequest.getQueueUrl());
                        continue;
                    } else {
                        // if we receive a message, we start processing
                        LOG.info("Received " + messages.size() + " messages");
                        Consumer<QueueMessage> consumer = message -> {
                            Long startExecution = System.currentTimeMillis();
                            // we invoke the method
                            String response = pollingRequest.getProcesser().apply(message.getMessage());
                            // if the response was not null we send it to the source queue according to its
                            // signature
                            if (Objects.nonNull(response)) {
                                LOG.infov("Sending response: {0} {1} {2}", response, message.getSourceQueueUrl(),
                                        message.getSignature());
                                sendAnswer(message.getSourceQueueUrl(), response, message.getSignature());
                            }
                            // if the execution time was lower than the min expected, sleep
                            Long currentTime = System.currentTimeMillis();
                            if (currentTime - startExecution < pollingRequest.getMinExecutionTime()) {
                                LOG.infov("Waiting for {0} ms",
                                        pollingRequest.getMinExecutionTime() - (currentTime - startExecution));
                                try {
                                    Thread.sleep(pollingRequest.getMinExecutionTime() - (currentTime - startExecution));
                                } catch (InterruptedException e) {
                                    LOG.error("Interrupted while waiting for minimum execution time");
                                    e.printStackTrace();
                                }
                            }
                        };
                        // if we configured parallelism, we use it
                        if (pollingRequest.isParallelProcessing()) {
                            messages.parallelStream().forEach(consumer);
                        } else {
                            messages.stream().forEach(consumer);
                        }
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("Queue " + pollingRequest.getQueueUrl() + " was stopped due to an error", e);
            }
        });

    }
}
