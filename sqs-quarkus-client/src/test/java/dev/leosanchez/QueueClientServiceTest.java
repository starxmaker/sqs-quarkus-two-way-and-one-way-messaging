package dev.leosanchez;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import dev.leosanchez.DTO.QueueMessage;
import dev.leosanchez.adapters.QueueClientAdapter.IQueueClientAdapter;
import dev.leosanchez.services.QueueService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;

@QuarkusTest
public class QueueServiceTest {

    // the response queue url
    String targetQueueUrl="https://abc.amazonaws.com/myQueue";
    
    // the class that we want to test
    @Inject
    QueueService queueService;

    // a mock of the sdk client
    @InjectMock
    IQueueClientAdapter queueClientAdapter;

    // here we are going to mock some responses of the sdk client
    @BeforeEach
    public void beforeEach() {
        // mock creation
        Mockito.when(
            queueClientAdapter.createQueue(Mockito.anyString())
        ).thenReturn("http://queue.url");

        Mockito.when(
            queueClientAdapter.receiveMessages(Mockito.eq("http://queue.url"), Mockito.anyInt())
        ).thenReturn(
            List.of(
                new QueueMessage("Au revoir", "FR_00000001", new HashMap<String, String>() {
                    {
                        put("Signature", "FR");
                    }
                }),
                new QueueMessage("Good bye", "EN_00000001", new HashMap<String, String>() {
                    {
                        put("Signature", "EN");
                    }
                })
            )      
        );
    }

    @Test
    public void testQueueCreation() {
        // we call our provider class (this is going to be called  @PostConstruct)
        queueService.createResponseQueue();
        // we verify that the request has been made succesfully
        Mockito.verify(queueClientAdapter).createQueue(argThat((ArgumentMatcher<String>) matcher ->  matcher.startsWith("TEST_RQ_TEMP_")));
    }

    // we are not actually deleting the queue, so it does not matter the order
    @Test
    public void testQueueDestruction() {
        // we call our provider class (this is going to be called  @PreDestroy)
        queueService.deleteResponseQueue();
        Mockito.verify(queueClientAdapter, times(1)).deleteQueue(Mockito.eq("http://queue.url"));
    }

    @Test
    public void testSendMessage(){
        // declaration of what are we going to send
        String message = "Bonjour";
        // we receive a signature from the method
        String signature = queueService.sendMessage(targetQueueUrl, message);
        Mockito.verify(queueClientAdapter, Mockito.times(1)).sendMessage(
            Mockito.eq(targetQueueUrl),
            Mockito.eq(message),
            argThat((ArgumentMatcher<Map<String, String>>) matcher -> matcher.get("Signature").equals(signature) && matcher.get("ResponseQueueUrl").equals("http://queue.url")
        ));
    }

    @Test
    public void testAwaitResponseSimple(){
        // we declare what we expect to receive (we already configured the mock to generate the same values)
        String signature = "EN";
        String expectedResponse = "Good bye";
        // we call our provider
        String response = queueService.receiveResponse(signature, 10);
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
        String response = queueService.receiveResponse(signature, 10);
        String responseConcurrent = queueService.receiveResponse(signatureConcurrent, 10);

        // we verify that the service was called only once
        Mockito.verify(queueClientAdapter, Mockito.times(1)).receiveMessages(Mockito.anyString(), Mockito.anyInt());

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
        String response = queueService.receiveResponse(signature, timeoutSeconds);
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
        queueService.receiveResponse(signature, 10);

        // we verify that a removal was requested with the right parameters
        Mockito.verify(queueClientAdapter, Mockito.times(1)).deleteMessage(Mockito.eq(responseQueueUrl), Mockito.eq(receiptHandle));
    }

}
