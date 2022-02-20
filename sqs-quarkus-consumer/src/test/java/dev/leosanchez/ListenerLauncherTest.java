package dev.leosanchez;


import static org.mockito.ArgumentMatchers.argThat;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import dev.leosanchez.DTO.QueueMessage;
import dev.leosanchez.listeners.CoordinatesListener;
import dev.leosanchez.listeners.ListenerLauncher;
import dev.leosanchez.providers.QueueConsumerProvider.IQueueConsumerProvider;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.vertx.core.json.JsonObject;

@QuarkusTest
public class ListenerLauncherTest {

    @ConfigProperty(name = "twoways.queue.url")
    String twoWaysQueueUrl;
    
    @ConfigProperty(name = "oneway.queue.url")
    String onewayQueueUrl;

    @Inject
    ListenerLauncher listenerLauncher;

    @InjectMock
    IQueueConsumerProvider queueProvider;

    @BeforeEach
    public void setup() {
        Mockito
            .when(queueProvider.pollMessages(argThat(matcher -> matcher.equals(twoWaysQueueUrl) || matcher.equals(onewayQueueUrl))))
            .then((invocation) -> {
                // we wait a second
                Thread.sleep(1000);

                // depending of the queue we return different messages
                List<QueueMessage> messages = new ArrayList<>();
                if (invocation.getArgument(0).equals(twoWaysQueueUrl)) {
                    messages.add(new QueueMessage("{\"city\":\"Santiago\"}","https://temp1.com", "SANTIAGO"));
                    messages.add(new QueueMessage("{\"city\":\"Coquimbo\"}","https://temp2.com", "COQUIMBO"));
                    messages.add(new QueueMessage("{\"city\":\"Punta Arenas\"}","https://temp2.com", "PUNTA_ARENAS"));
                } else {
                    JsonObject json = new JsonObject();
                    json.put("name", "Temuco");
                    json.put("lat", -41.5);
                    json.put("lon", -72.9);
                    messages.add(new QueueMessage(json.encode(),"https://temp1.com", "TEMUCO"));
                }
                return messages;
            });
    }


    @Test
    public void testListenerQuantityGetter() {
        Class<?> coordinatesListener = CoordinatesListener.class;
        Integer actualQuantity = 2;
        Assertions.assertEquals(actualQuantity, listenerLauncher.getNumberOfListeners(List.of(coordinatesListener)));
    }

    @Test
    public void testMessagePolling () {
        Class<?> coordinatesListener = CoordinatesListener.class;
        listenerLauncher.startListenerClasses(List.of(coordinatesListener));
        Mockito.verify(queueProvider, Mockito.timeout(3000).times(3)).pollMessages(argThat( (ArgumentMatcher<String>) matcher -> matcher.equals(onewayQueueUrl)));
    }

    @Test
    public void testBidirectionalMessageProcessing() {
        Class<?> coordinatesListener = CoordinatesListener.class;
        listenerLauncher.startListenerClasses(List.of(coordinatesListener));
        Mockito.verify(queueProvider, Mockito.timeout(5000).atLeastOnce()).sendAnswer(Mockito.eq("https://temp1.com"), Mockito.anyString(), Mockito.eq("SANTIAGO"));
        Mockito.verify(queueProvider, Mockito.timeout(5000).atLeastOnce()).sendAnswer(Mockito.eq("https://temp2.com"), Mockito.anyString(), Mockito.eq("COQUIMBO"));
    }

    @Test
    public void testUnidirectionalMessageProcessing() throws InterruptedException {
        Class<?> coordinatesListener = CoordinatesListener.class;
        listenerLauncher.startListenerClasses(List.of(coordinatesListener));
        Thread.sleep(2000);
        Mockito.verify(queueProvider, Mockito.times(0)).sendAnswer(Mockito.eq("https://temp1.com"), Mockito.anyString(), Mockito.eq("TEMUCO"));
    }


}
