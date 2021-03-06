package dev.leosanchez;

import static org.mockito.ArgumentMatchers.argThat;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.inject.Inject;

import dev.leosanchez.common.dto.QueueMessage;
import dev.leosanchez.common.exceptions.MessagePollingException;
import dev.leosanchez.common.exceptions.MessageSendingException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import dev.leosanchez.DTO.ListenRequest;
import dev.leosanchez.listeners.IListener;
import dev.leosanchez.services.ListenerLauncherService;
import dev.leosanchez.services.QueueConsumerService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;

@QuarkusTest
public class ListenerLauncherTest {
    // our service that launches the listeners
    @Inject
    ListenerLauncherService listenerLauncher;

     // our service that communicates with the queue provider
    // we dont want to actually call it so we mock it
    @InjectMock
    QueueConsumerService queueConsumerService;

    // mocks objects based on the IListener interface. As interfaces, we will initialize them later
    IListener oneWayListenerMock;
    IListener twoWaysListenerMock;

    @BeforeEach
    public void beforeEach() throws MessagePollingException {
        // we initialize our listener mocks
        oneWayListenerMock = Mockito.mock(IListener.class);
        twoWaysListenerMock = Mockito.mock(IListener.class);
        
        // we simulate a response for our listener mocks when the process method is called
        Mockito.when(twoWaysListenerMock.process(Mockito.anyString())).thenReturn(Optional.of("Chao"));
        Mockito.when(oneWayListenerMock.process(Mockito.anyString())).thenReturn(Optional.empty());

        // we will simulate the poll function of our queue consumer provider
        Mockito.when(queueConsumerService.pollMessages(Mockito.anyString(), Mockito.anyInt()))
            .thenAnswer(invocations -> {
                // The messages we will receive will have the following format
                // sourceQueueName + "/responseQueue"
                String responseQueueUrl = invocations.getArgument(0) +"/responseQueue";
                // we create a list of messages that our mock will return
                return List.of(
                    new QueueMessage("Hola", "ES_00000001", new HashMap<String, String>() {{
                        put("Signature", "ES");
                        put("ResponseQueueUrl", responseQueueUrl);
                    }}),
                    new QueueMessage("Hi", "EN_00000001", new HashMap<String, String>() {{
                        put("Signature", "EN");
                        put("ResponseQueueUrl", responseQueueUrl);
                    }}),
                    new QueueMessage("Ciao", "IT_00000001", new HashMap<String, String>() {{
                        put("Signature", "IT");
                        put("ResponseQueueUrl", responseQueueUrl);
                    }})
                );
        });
    }


    @Test
    public void testTwoWaysListening() throws MessageSendingException, MessagePollingException{
        // define listen request
        ListenRequest listenRequest = new ListenRequest(twoWaysListenerMock,  "FirstMock", false, 10, 0);
        // we will launch polling three times
        Integer numberOfRequests = 3;

        // we launch the orchestration
        listenerLauncher.orchestrateListeners( List.of(listenRequest), numberOfRequests);

        // in three pollings we expect three calls to our provider
        Mockito.verify(queueConsumerService, Mockito.times(3)).pollMessages("FirstMock", 10);
        // we verify that the processer and sender has been called three times as well
        Mockito.verify(queueConsumerService, Mockito.times(3)).sendAnswer(Mockito.eq("FirstMock/responseQueue"), Mockito.eq("Chao"), Mockito.eq("ES"));
    }


    
    @Test
    public void testOneWayListening() throws MessageSendingException, MessagePollingException{
        //define listen request
        ListenRequest listenRequest = new ListenRequest(oneWayListenerMock,  "SecondMock", false, 10, 0);
        Integer numberOfRequests = 3;

        listenerLauncher.orchestrateListeners( List.of(listenRequest), numberOfRequests);
        
        // in those three pollings we expect three calls to our provider
        Mockito.verify(queueConsumerService, Mockito.atLeast(3)).pollMessages("SecondMock", 10);
        // as those messages do not expect response, we verify that the send answer method has not been called
        Mockito.verify(queueConsumerService, Mockito.never()).sendAnswer(Mockito.eq("SecondMock/responseQueue"), Mockito.any(), Mockito.anyString());
    }
    
    @Test
    public void testNoParallelProcessing() throws InterruptedException, ExecutionException, MessageSendingException {
        //we define the listener request with non parallel processing
        ListenRequest listenRequest = new ListenRequest(twoWaysListenerMock,  "ThirdMock", false, 10, 500);
        // we just require 1 polling to test this behaviour
        Integer numberOfRequests = 1;

        // we will call this listener asyncronously because we want to check the calls at an specifiy point of time during its execution
        Future<?> task = CompletableFuture.runAsync(() -> {
            listenerLauncher.orchestrateListeners(List.of(listenRequest), numberOfRequests);
        });
        Thread.sleep(500); // 500 ms for the processing of one message
        // there should be just one message processed as messages are processed in a sequence
        Mockito.verify(queueConsumerService, Mockito.times(1)).sendAnswer(Mockito.eq("ThirdMock/responseQueue"), Mockito.anyString(), argThat(arg-> arg.equals("ES") || arg.equals("EN") || arg.equals("IT")));
        // we wait the task to finish
        task.get();
    }

    @Test
    public void testParallelProcessing() throws InterruptedException, ExecutionException, MessageSendingException {
        // we define the listener request with parallel processing
        ListenRequest listenRequest = new ListenRequest(twoWaysListenerMock,  "FourthMock", true, 10, 500);
        Integer numberOfRequests = 1;
        // we call the listener async because we want to check the calls at an specifict point of time
        Future<?> task = CompletableFuture.runAsync(() -> {
            listenerLauncher.orchestrateListeners(List.of(listenRequest), numberOfRequests);
        });
        Thread.sleep(500);  
        // as process are processed in parallel, we expect more that one message processed during those 500 ms
        Mockito.verify(queueConsumerService, Mockito.atLeast(2)).sendAnswer(Mockito.eq("FourthMock/responseQueue"), Mockito.anyString(), argThat(arg-> arg.equals("ES") || arg.equals("EN") || arg.equals("IT")));
        // we wait the task to finish
        task.get();
    } 

    @Test
    public void testMultipleListeners() throws MessageSendingException, MessagePollingException {
         // define multiple listen requests
         ListenRequest listenRequest = new ListenRequest(twoWaysListenerMock,  "FifthMock", false, 10, 0);
         ListenRequest secondRequest = new ListenRequest(twoWaysListenerMock,  "SixthMock", false, 10, 0);
         Integer numberOfRequests = 3;
 
         // we launch the orchestration
         listenerLauncher.orchestrateListeners( List.of(listenRequest, secondRequest), numberOfRequests);
 
         // in three polling requests we expect three calls for each listener
         Mockito.verify(queueConsumerService, Mockito.times(3)).pollMessages("FifthMock", 10);
         Mockito.verify(queueConsumerService, Mockito.times(3)).pollMessages("SixthMock", 10);
         // we verify that the processer and sender has been called three times
         Mockito.verify(queueConsumerService, Mockito.times(3)).sendAnswer(Mockito.eq("FifthMock/responseQueue"), Mockito.eq("Chao"), Mockito.eq("ES"));
         Mockito.verify(queueConsumerService, Mockito.times(3)).sendAnswer(Mockito.eq("SixthMock/responseQueue"), Mockito.eq("Chao"), Mockito.eq("ES"));
    }

}
