package dev.leosanchez;


import javax.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import dev.leosanchez.DTO.QueueMessage;
import dev.leosanchez.DTO.PollingRequest.PollingRequestBuilder;
import dev.leosanchez.providers.QueueConsumerProvider.SQSConsumerProvider;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeast;

import java.util.HashMap;
import java.util.List;

@QuarkusTest
public class SQSConsumerProviderTest {

    String queueUrl = "https://parentQueue.com/testQueue";
   
    @Inject
    SQSConsumerProvider sqsProvider;
    
    @InjectMock
    SqsClient sqs;

    @BeforeEach
    public void beforeEach() {
        // mock message receive
        Mockito.when(sqs.receiveMessage(argThat((ArgumentMatcher<ReceiveMessageRequest>) matcher -> {
            // we verify that the request has been made with the right parameters
            return matcher.queueUrl().equals(queueUrl) &&
                    matcher.messageAttributeNames().contains("All") &&
                    matcher.waitTimeSeconds().equals(20);
        })))
            .then( invocations -> {
                Thread.sleep(1000);
                // we return a mocked response
                return ReceiveMessageResponse.builder().messages(
                    List.of(
                        Message.builder()
                            .body("Au revoir")
                            .receiptHandle("FR_00000001")
                            .messageAttributes(
                                new HashMap<String, MessageAttributeValue> () {{
                                    put("Signature",MessageAttributeValue.builder().stringValue("FR").build());
                                    put("ResponseQueueUrl", MessageAttributeValue.builder().stringValue("https://targetqueue.com/testQueue").build());
                                }}
                            )
                        .build(),
                        Message.builder()
                        .body("Good bye")
                        .receiptHandle("EN_00000001")
                        .messageAttributes(
                            new HashMap<String, MessageAttributeValue> () {{
                                put("Signature",MessageAttributeValue.builder().stringValue("EN").build());
                                put("ResponseQueueUrl", MessageAttributeValue.builder().stringValue("https://targetqueue.com/testQueue").build());
                            }}
                        )
                    .build()
                    )
                )
                .build(); }
        );
    }

    @Test
    public void pollMessages() {
        List<QueueMessage> messages = sqsProvider.pollMessages(queueUrl, 10);
        Assertions.assertEquals(2, messages.size());
        Assertions.assertEquals("Au revoir", messages.get(0).getMessage());
        Assertions.assertEquals("https://targetqueue.com/testQueue", messages.get(0).getSourceQueueUrl());
        Assertions.assertEquals("FR", messages.get(0).getSignature());
        Assertions.assertEquals("Good bye", messages.get(1).getMessage());
        Assertions.assertEquals("https://targetqueue.com/testQueue", messages.get(1).getSourceQueueUrl());
        Assertions.assertEquals("EN", messages.get(1).getSignature());
    }

    @Test
    public void deleteMessages() {
        sqsProvider.pollMessages(queueUrl, 10);
        Mockito.verify(sqs, Mockito.atLeastOnce()).deleteMessage(argThat((ArgumentMatcher<DeleteMessageRequest>) matcher -> {
            return matcher.receiptHandle().equals("FR_00000001");
        }));

        Mockito.verify(sqs, Mockito.atLeastOnce()).deleteMessage(argThat((ArgumentMatcher<DeleteMessageRequest>) matcher -> {
            return matcher.receiptHandle().equals("EN_00000001");
        }));
    }

    @Test
    public void sendAnswer() {
        sqsProvider.sendAnswer("https://targetqueue.com/testQueue", "Hello", "FR");
        Mockito.verify(sqs, Mockito.times(1)).sendMessage(argThat((ArgumentMatcher<software.amazon.awssdk.services.sqs.model.SendMessageRequest>) matcher -> {
            return (matcher.queueUrl().equals("https://targetqueue.com/testQueue") &&
                    matcher.messageBody().equals("Hello") &&
                    matcher.messageAttributes().containsKey("Signature") &&
                    matcher.messageAttributes().get("Signature").stringValue().equals("FR"));
        }));
    }

    @Test
    public void testTwoWaysListening() {
        // we call the listener
        sqsProvider.listen(PollingRequestBuilder.builder()
            .queueUrl(queueUrl)
            .processer((message) -> "Hola")
            .maxNumberOfMessagesPerPolling(5)
        .build());
        
        // we wait 3 seconds and we expect at least three calls (assuming that each call took 1 second)
        Mockito.verify(sqs, Mockito.timeout(3000).atLeast(3)).receiveMessage(argThat((ArgumentMatcher<ReceiveMessageRequest>) matcher -> {
            return (matcher.queueUrl().equals(queueUrl) &&
                    matcher.maxNumberOfMessages().equals(5));
        }));
        // we verify that the processer and sender has been called
        Mockito.verify(sqs, atLeast(1)).sendMessage(argThat((ArgumentMatcher<SendMessageRequest>) matcher -> {
            return (matcher.messageBody().equals("Hola"));
        }));
    }

    @Test
    public void testOneWayListening() throws InterruptedException{
        // we call the listener with a processer that returns null
        sqsProvider.listen(PollingRequestBuilder.builder()
            .queueUrl(queueUrl)
            .processer((message) -> null)
            .maxNumberOfMessagesPerPolling(5)
        .build());

        // we wait 3 seconds and we expect that the message sender is never called with a null message
        Thread.sleep(3000);
        Mockito.verify(sqs, Mockito.never()).sendMessage(argThat((ArgumentMatcher<SendMessageRequest>) matcher -> {
            return (matcher.messageBody().equals(null));
        }));
    }

    @Test
    public void testNoParallelProcessing() throws InterruptedException {
        // we call the listener with no parallel processing and a min execution time of 500ms
        sqsProvider.listen(PollingRequestBuilder.builder()
            .queueUrl(queueUrl)
            .processer((message) -> "Bonjour")
            .maxNumberOfMessagesPerPolling(2)
            .noParallelProcessing()
            .minExecutionTime(500)
        .build());

        Thread.sleep(1000); // polling delay;
        Thread.sleep(500); // min execution time;
        // there should be just one message processed
        Mockito.verify(sqs, Mockito.times(1)).sendMessage(argThat((ArgumentMatcher<SendMessageRequest>) matcher -> {
            return (matcher.messageBody().equals("Bonjour"));
        }));
    }

    @Test
    public void testParallelProcessing() throws InterruptedException {
        // we call the listener with parallel processing
        sqsProvider.listen(PollingRequestBuilder.builder()
            .queueUrl(queueUrl)
            .processer((message) -> "Hi")
            .maxNumberOfMessagesPerPolling(2)
            .minExecutionTime(500)
        .build());

        Thread.sleep(1000); // polling delay;
        Thread.sleep(500); // min execution time;
        // there should be two messages processed
        Mockito.verify(sqs, Mockito.times(2)).sendMessage(argThat((ArgumentMatcher<SendMessageRequest>) matcher -> {
            return (matcher.messageBody().equals("Hi"));
        }));
    }
}
