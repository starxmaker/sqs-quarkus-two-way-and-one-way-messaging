package dev.leosanchez;

import static org.mockito.ArgumentMatchers.argThat;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import javax.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import dev.leosanchez.DTO.QueueMessage;
import dev.leosanchez.listeners.IListener;
import dev.leosanchez.listeners.ListenerLauncher;
import dev.leosanchez.providers.QueueConsumerProvider.IQueueConsumerProvider;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;

@QuarkusTest
public class ListenerLauncherTest {
     
    @InjectMock
    IQueueConsumerProvider queueConsumerProvider;

    @Inject
    ListenerLauncher listenerLauncher;

    IListener oneWayListenerMock;
    IListener twoWaysListenerMock;


    @BeforeEach
    public void beforeEach() {

        oneWayListenerMock = Mockito.mock(IListener.class);
        twoWaysListenerMock = Mockito.mock(IListener.class);
        
        // we create a mock for the two way listener
        Mockito.when(twoWaysListenerMock.process(Mockito.anyString())).thenReturn(Optional.of("Chao"));

         // we create a mock for the one way listener
         Mockito.when(oneWayListenerMock.process(Mockito.anyString())).thenReturn(Optional.empty());
        
        // mock message receive
        Mockito.when(queueConsumerProvider.pollMessages(Mockito.anyString(), Mockito.anyInt()))
            .thenAnswer(invocations -> {
                //mocking a call time
                Thread.sleep(1000);
                String responseQueueUrl = invocations.getArgument(0) +"/responseQueue";
                System.out.println(responseQueueUrl);
                return List.of(
                    new QueueMessage("Hola", responseQueueUrl, "ES"),
                    new QueueMessage("Hi",  responseQueueUrl, "EN"),
                    new QueueMessage("Ciao", responseQueueUrl, "IT")
                );
        });
    }


    @Test
    public void testTwoWaysListening() {
        Executors.newSingleThreadExecutor().submit(() -> listenerLauncher.startListener(twoWaysListenerMock, "FirstMock", false, 10, 0));
       
        // we wait 3 seconds and we expect at least three calls (assuming that each call took 1 second)
        Mockito.verify(queueConsumerProvider, Mockito.timeout(3000).atLeast(3)).pollMessages("FirstMock", 10);
        // we verify that the processer and sender has been called
        Mockito.verify(queueConsumerProvider, Mockito.timeout(3000).atLeast(3)).sendAnswer(Mockito.eq("FirstMock/responseQueue"), Mockito.eq("Chao"), Mockito.eq("ES"));
    }

    
    @Test
    public void testOneWayListening() throws InterruptedException{
        Executors.newSingleThreadExecutor().submit(() -> listenerLauncher.startListener(oneWayListenerMock, "SecondMock", false, 10, 0));
        
        // we verify that the sender has never been called
        Thread.sleep(3000);
        Mockito.verify(queueConsumerProvider, Mockito.never()).sendAnswer(Mockito.eq("SecondMock/responseQueue"), Mockito.any(), Mockito.anyString());
    }
    
    
    @Test
    public void testNoParallelProcessing() throws InterruptedException {
        // we call the listener with no parallel processing and a min execution time of 500ms
        Executors.newSingleThreadExecutor().submit(() -> listenerLauncher.startListener(twoWaysListenerMock, "FourthMock", false, 10, 500));

        Thread.sleep(1000); // polling delay;
        Thread.sleep(500); // min execution time;
        // there should be just one message processed
        Mockito.verify(queueConsumerProvider, Mockito.times(1)).sendAnswer(Mockito.eq("FourthMock/responseQueue"), Mockito.anyString(), argThat(arg-> arg.equals("ES") || arg.equals("EN") || arg.equals("IT")));
    }

    @Test
    public void testParallelProcessing() throws InterruptedException {
        // we call the listener with no parallel processing and a min execution time of 500ms
        Executors.newSingleThreadExecutor().submit(() -> listenerLauncher.startListener(twoWaysListenerMock, "FifthMock", true, 10, 500));

        Thread.sleep(1000); // polling delay;
        Thread.sleep(500); // min execution time;
        // there should be just one message processed
        Mockito.verify(queueConsumerProvider, Mockito.times(3)).sendAnswer(Mockito.eq("FifthMock/responseQueue"), Mockito.anyString(), argThat(arg-> arg.equals("ES") || arg.equals("EN") || arg.equals("IT")));
    } 
}
