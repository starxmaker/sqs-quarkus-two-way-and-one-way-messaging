package dev.leosanchez;


import javax.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import dev.leosanchez.DTO.QueueMessage;
import dev.leosanchez.adapters.QueueAdapter.IQueueAdapter;
import dev.leosanchez.services.QueueConsumerService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@QuarkusTest
public class QueueConsumerServiceTest {

    String queueUrl = "https://parentQueue.com/testQueue";
   
    @Inject
    QueueConsumerService service;
    
    @InjectMock
    IQueueAdapter adapter;

    @BeforeEach
    public void beforeEach() {
        // mock message receive
        Mockito.when(
            adapter.receiveMessages(Mockito.eq(queueUrl), Mockito.anyInt())
        ).thenReturn(
            List.of(
                new QueueMessage("Au revoir", "FR_00000001", new HashMap<String, String>() {
                    {
                        put("Signature","FR");
                        put("ResponseQueueUrl", "https://targetqueue.com/testQueue");
                    }
                }),
                new QueueMessage("Good bye", "EN_00000001", new HashMap<String, String>() {
                    {
                        put("Signature","EN");
                        put("ResponseQueueUrl", "https://targetqueue.com/testQueue");
                    }
                })
            )
        );
    }

    @Test
    public void pollMessages() {
        List<QueueMessage> messages =service.pollMessages(queueUrl, 10);
        Assertions.assertEquals(2, messages.size());
        Assertions.assertEquals("Au revoir", messages.get(0).getMessage());
        Assertions.assertEquals("https://targetqueue.com/testQueue", messages.get(0).getAttributes().get("ResponseQueueUrl"));
        Assertions.assertEquals("FR", messages.get(0).getAttributes().get("Signature"));
        Assertions.assertEquals("Good bye", messages.get(1).getMessage());
        Assertions.assertEquals("https://targetqueue.com/testQueue", messages.get(1).getAttributes().get("ResponseQueueUrl"));
        Assertions.assertEquals("EN", messages.get(1).getAttributes().get("Signature"));
    }

    @Test
    public void deleteMessages() {
        service.pollMessages(queueUrl, 10);
        Mockito.verify(adapter, Mockito.atLeastOnce()).deleteMessage(Mockito.eq(queueUrl), Mockito.eq("FR_00000001"));
        Mockito.verify(adapter, Mockito.atLeastOnce()).deleteMessage(Mockito.eq(queueUrl), Mockito.eq("EN_00000001"));
    }

    @Test
    public void sendAnswer() {
        service.sendAnswer("https://targetqueue.com/testQueue", "Hello", "FR");
        Mockito.verify(adapter, Mockito.times(1)).sendMessage(
            Mockito.eq("https://targetqueue.com/testQueue"),
            Mockito.eq("Hello"),
            Mockito.argThat((ArgumentMatcher<Map<String, String>>) matcher -> matcher.get("Signature").equals("FR"))
        );    
    }

}
