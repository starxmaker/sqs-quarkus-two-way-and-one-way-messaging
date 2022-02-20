package dev.leosanchez;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;

import java.util.HashMap;
import java.util.List;

import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import dev.leosanchez.providers.QueueClientProvider.SQSClientProvider;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@QuarkusTest
public class SQSClientProviderTest {

    // the response queue url
    String targetQueueUrl="https://abc.amazonaws.com/myQueue";
    
    // the class that we want to test
    @Inject
    SQSClientProvider sqsProvider;

    // a mock of the sdk client
    @InjectMock
    SqsClient sqs;

    // here we are going to mock some responses of the sdk client
    @BeforeEach
    public void beforeEach() {
        // mock creation
        Mockito.when(
            sqs.createQueue(Mockito.any(CreateQueueRequest.class))
        ).thenReturn(CreateQueueResponse.builder().queueUrl("http://queue.url").build());

        // mock message receive
        Mockito.when(sqs.receiveMessage(argThat((ArgumentMatcher<ReceiveMessageRequest>) matcher -> {
            // we verify that the request has been made with the right parameters
            return matcher.queueUrl().equals("http://queue.url") &&
                    matcher.messageAttributeNames().contains("All") &&
                    matcher.waitTimeSeconds().equals(20) && // we verify that long polling is enabled
                    matcher.maxNumberOfMessages().equals(10);
        })))
            .thenReturn(
                // we return a mocked response
                ReceiveMessageResponse.builder().messages(
                    List.of(
                        Message.builder()
                            .body("Au revoir")
                            .receiptHandle("FR_00000001")
                            .messageAttributes(
                                new HashMap<String, MessageAttributeValue> () {{
                                    put("Signature",MessageAttributeValue.builder().stringValue("FR").build());
                                }}
                            )
                        .build(),
                        Message.builder()
                        .body("Good bye")
                        .receiptHandle("EN_00000001")
                        .messageAttributes(
                            new HashMap<String, MessageAttributeValue> () {{
                                put("Signature",MessageAttributeValue.builder().stringValue("EN").build());
                            }}
                        )
                    .build()
                    )
                )
                .build()
        );
    }

    @Test
    public void testQueueCreation() {
        // we call our provider class (this is going to be called  @PostConstruct)
        sqsProvider.createResponseQueue();
        // we verify that the request has been made succesfully
        Mockito.verify(sqs).createQueue(argThat((ArgumentMatcher<CreateQueueRequest>) matcher ->  matcher.queueName().startsWith("TEST_RQ_TEMP_")));
    }

    // we are not actually deleting the queue, so it does not matter the order
    @Test
    public void testQueueDestruction() {
        // we call our provider class (this is going to be called  @PreDestroy)
        sqsProvider.deleteResponseQueue();
        Mockito.verify(sqs, times(1)).deleteQueue(argThat((ArgumentMatcher<DeleteQueueRequest>) matcher -> matcher.queueUrl().equals("http://queue.url")));

    }

    @Test
    public void testSendMessage(){
        // declaration of what are we going to send
        String message = "Bonjour";
        // we receive a signature from the method
        String signature = sqsProvider.sendMessage(targetQueueUrl, message);
        Mockito.verify(sqs, Mockito.times(1)).sendMessage(argThat((ArgumentMatcher<SendMessageRequest>) matcher -> {
            // we verify that the message is the one we sent
            return matcher.messageBody().equals(message) &&
                    matcher.queueUrl().equals(targetQueueUrl) &&
                    matcher.messageAttributes().get("Signature").stringValue().equals(signature) &&
                    matcher.messageAttributes().get("ResponseQueueUrl").stringValue().equals("http://queue.url");
        }));
    }

    @Test
    public void testAwaitResponseSimple(){
        // we declare what we expect to receive (we already configured the mock to generate the same values)
        String signature = "EN";
        String expectedResponse = "Good bye";
        // we call our provider
        String response = sqsProvider.receiveResponse(signature, 10);
        // we verify that the mocked response is returned
        Assertions.assertEquals(response, expectedResponse);
    }

    @Test
    public void testAwaitResponseConcurrent(){
        // we declare what we expect to receive (we already configured the mock to generate the same values)
        String signature = "EN";
        String signatureConcurrent= "FR";
        String expectedResponse = "Good bye";
        String expectedResponseConcurrent = "Au revoir";
        
        // we call our provider
        String response = sqsProvider.receiveResponse(signature, 10);
        String responseConcurrent = sqsProvider.receiveResponse(signatureConcurrent, 10);

        // we verify that the service was called only once
        Mockito.verify(sqs, Mockito.times(1)).receiveMessage(Mockito.any(ReceiveMessageRequest.class));

        // we verify that the mocked response is returned
        Assertions.assertEquals(response, expectedResponse);
        Assertions.assertEquals(responseConcurrent, expectedResponseConcurrent);
    }
    
    @Test
    public void testAwaitResponseTimeout() {
        // a non existing signature (according to what we have declared)
        String signature = "ES";

        // more than enought to check timeout in unit testing
        Integer timeoutSeconds = 1;

        // we register when we start the method
        Long start = System.currentTimeMillis();
        // we call the method
        String response = sqsProvider.receiveResponse(signature, timeoutSeconds);
        // we register the end of the execution
        Long end = System.currentTimeMillis();

        // we verify that the response is null
        Assertions.assertNull(response);
        // we verify that the execution time is greater or equal than the specified timeout
        
        // we will round, because some milliseconds could pass after or before the execution
        Assertions.assertTrue(Math.floorDiv(end-start, 1000) == timeoutSeconds);   
    }
    
    @Test
    public void testMessageRemoval(){

        // we declare a message that is previously declared in the mock to be received (as removal happens after receiving)
        String signature = "FR";
        String receiptHandle = "FR_00000001";
        String responseQueueUrl = "http://queue.url";
        
        // we call our class
        sqsProvider.receiveResponse(signature, 10);

        // we verify that a removal was requested with the right parameters
        Mockito.verify(sqs, Mockito.times(1)).deleteMessage(argThat((ArgumentMatcher<DeleteMessageRequest>) matcher -> {
            return matcher.queueUrl().equals(responseQueueUrl) &&
                    matcher.receiptHandle().equals(receiptHandle);
        }));
    }

}
