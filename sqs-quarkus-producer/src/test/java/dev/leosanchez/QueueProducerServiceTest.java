package dev.leosanchez;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import dev.leosanchez.common.dto.QueueMessage;
import dev.leosanchez.common.adapters.queueadapter.IQueueAdapter;
import dev.leosanchez.common.exceptions.MessagePollingException;
import dev.leosanchez.common.exceptions.MessageRemovalException;
import dev.leosanchez.common.exceptions.MessageSendingException;
import dev.leosanchez.common.exceptions.QueueCreationException;
import dev.leosanchez.common.exceptions.QueueRemovalException;
import dev.leosanchez.producer.services.QueueProducerService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;

@QuarkusTest
public class QueueProducerServiceTest {
    
    // the class that we want to test
    @Inject
    QueueProducerService queueService;

    // a mock of the sdk client
    @InjectMock
    IQueueAdapter queueClientAdapter;

    // here we are going to mock some responses of the sdk client
    @BeforeEach
    public void beforeEach() throws MessagePollingException{
        // mock creation

        Mockito.when(
            queueClientAdapter.receiveMessages(Mockito.anyString(), Mockito.anyInt())
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
    public void testQueueCreation() throws QueueCreationException {
        // we call our provider class (this is going to be called  @PostConstruct)
        queueService.createResponseQueue();
        // we verify that the request has been made succesfully
        Mockito.verify(queueClientAdapter).createQueue(argThat((ArgumentMatcher<String>) matcher ->  matcher.startsWith("TEST_RQ_TEMP_")));
    }

    @Test
    public void testQueueDestruction() throws QueueRemovalException {
        // we call our provider class (this is going to be called  @PreDestroy)
        queueService.deleteResponseQueue();
        Mockito.verify(queueClientAdapter, times(1)).deleteQueue(Mockito.anyString());
        // we create the response queue again
        queueService.createResponseQueue();
    }

    @Test
    public void testSendMessage() throws MessageSendingException{
        // declaration of what are we going to send
        String message = "Bonjour";
        // we receive a signature from the method
        String signature = queueService.sendMessageForResponse("ABC", message);
        Mockito.verify(queueClientAdapter, times(1)).sendMessageWithAttributes(
            Mockito.eq("ABC"),
            Mockito.eq(message),
            argThat((ArgumentMatcher<Map<String, String>>) matcher -> matcher.get("Signature").equals(signature) && matcher.get("ResponseQueueUrl").contains("TEST_RQ_TEMP_"))
        );
    }

    @Test
    public void testAwaitResponseSimple(){
        // we declare what we expect to receive (we already configured the mock to generate the same values)
        String signature = "EN";
        String expectedResponse = "Good bye";
        // we call our provider
        Optional<String> response = queueService.receiveResponse(signature, 10);
        // we verify that the mocked response is returned
        Assertions.assertEquals(response.get(), expectedResponse);
    }

    @Test
    public void testAwaitResponseConcurrent() throws MessagePollingException{
        // we declare what we expect to receive (we already configured the mock to generate the same values)
        String signature = "EN";
        String signatureConcurrent= "FR";
        String expectedResponse = "Good bye";
        String expectedResponseConcurrent = "Au revoir";
        
        // we call our provider
        Optional<String> response = queueService.receiveResponse(signature, 10);
        Optional<String> responseConcurrent = queueService.receiveResponse(signatureConcurrent, 10);

        // we verify that the service was called only once
        Mockito.verify(queueClientAdapter, Mockito.times(1)).receiveMessages(Mockito.anyString(), Mockito.anyInt());

        // we verify that the mocked response is returned
        Assertions.assertEquals(response.get(), expectedResponse);
        Assertions.assertEquals(responseConcurrent.get(), expectedResponseConcurrent);
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
        Optional<String> response = queueService.receiveResponse(signature, timeoutSeconds);
        // we register the end of the execution
        Long end = System.currentTimeMillis();

        // we verify that the response is null
        Assertions.assertTrue(response.isEmpty());
        // we verify that the execution time is greater or equal than the specified timeout
        
        // we will round, because some milliseconds could pass after or before the execution
        Assertions.assertTrue(Math.floorDiv(end-start, 1000) == timeoutSeconds);   
    }
    
    @Test
    public void testMessageRemoval() throws MessageRemovalException{

        // we declare a message that is previously declared in the mock to be received (as removal happens after receiving)
        String signature = "FR";
        String receiptHandle = "FR_00000001";
        
        // we call our class
        queueService.receiveResponse(signature, 10);

        // we verify that a removal was requested with the right parameters
        Mockito.verify(queueClientAdapter, Mockito.times(1)).deleteMessage(Mockito.eq(queueService.getResponseQueueUrl().get()), Mockito.eq(receiptHandle));
    }

}
