package dev.leosanchez;


import javax.inject.Inject;

import dev.leosanchez.common.dto.QueueMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import dev.leosanchez.common.adapters.queueadapter.IQueueAdapter;
import dev.leosanchez.common.exceptions.MessagePollingException;
import dev.leosanchez.common.exceptions.MessageRemovalException;
import dev.leosanchez.common.exceptions.MessageSendingException;
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
    public void beforeEach() throws MessagePollingException{
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
    public void pollMessages() throws MessagePollingException {
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
    public void deleteMessages() throws MessageRemovalException, MessagePollingException {
        service.pollMessages(queueUrl, 10);
        Mockito.verify(adapter, Mockito.atLeastOnce()).deleteMessage(Mockito.eq(queueUrl), Mockito.eq("FR_00000001"));
        Mockito.verify(adapter, Mockito.atLeastOnce()).deleteMessage(Mockito.eq(queueUrl), Mockito.eq("EN_00000001"));
    }

    @Test
    public void sendAnswer() throws MessageSendingException {
        service.sendAnswer("https://targetqueue.com/testQueue", "Hello", "FR");
        Mockito.verify(adapter, Mockito.times(1)).sendMessageWithAttributes(
            Mockito.eq("https://targetqueue.com/testQueue"),
            Mockito.eq("Hello"),
            Mockito.argThat((ArgumentMatcher<Map<String, String>>) matcher -> matcher.get("Signature").equals("FR"))
        );    
    }

}
