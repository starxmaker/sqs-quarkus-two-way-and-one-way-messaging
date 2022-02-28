package dev.leosanchez;


import javax.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import dev.leosanchez.DTO.QueueMessage;
import dev.leosanchez.providers.QueueConsumerProvider.SQSConsumerProvider;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import static org.mockito.ArgumentMatchers.argThat;

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

}
