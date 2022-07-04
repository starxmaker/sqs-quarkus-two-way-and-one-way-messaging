package dev.leosanchez;

import static org.mockito.ArgumentMatchers.argThat;

import java.util.Optional;

import javax.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import dev.leosanchez.common.exceptions.MessageSendingException;
import dev.leosanchez.producer.services.CoordinatesService;
import dev.leosanchez.producer.services.QueueProducerService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.vertx.core.json.JsonObject;

@QuarkusTest
public class CoordinatesServiceTest {
    
    // we inject the service we want to test
    @Inject
    CoordinatesService service;

    // we mock our provider
    @InjectMock
    QueueProducerService queueService;

    // we implement some mock methods
    @BeforeEach
    public void setup() throws MessageSendingException {
        JsonObject response = new JsonObject();
        response.put("lat", -34.397);
        response.put("lon", 150.644);

        // we configure some signature responses
        Mockito.when(queueService.sendMessageForResponse(
            Mockito.anyString(),
            argThat(matcher -> matcher.contains("Coquimbo") || matcher.contains("Santiago"))
        )).thenAnswer(answer -> {
            if (answer.getArgument(1).toString().contains("Coquimbo")) {
                return "CQBO";
            } else {
                return "STGO";
            }
        });
        // the first signature will return a response, the second will not
        Mockito.when(queueService.receiveResponse(
            argThat(matcher -> matcher.equals("CQBO") || matcher.equals("STGO")
        ), Mockito.anyInt())).thenAnswer(answer -> {
            if (answer.getArgument(0).equals("CQBO")) {
                return Optional.of(response.toString());
            } else {
                return Optional.empty();
            }
        });
    }

    @Test
    public void testQueryCoordinates() {
        Optional<JsonObject> response = service.queryCoordinates("Coquimbo");
        Assertions.assertEquals(response.get().getDouble("lat"), -34.397);
        Assertions.assertEquals(response.get().getDouble("lon"), 150.644);
    }

    @Test
    public void testNotFoundCoordinates() {
        Optional<JsonObject> response = service.queryCoordinates("Santiago");
        Assertions.assertTrue(response.isEmpty());
    }

    @Test
    public void testSubmitCoordinates() throws MessageSendingException {
        service.submitCoordinates("Santiago", -34.397, 150.644);
        Mockito.verify(queueService, Mockito.times(1)).sendMessageForNoResponse(
            Mockito.anyString(),
            argThat(matcher -> {
                JsonObject request = new JsonObject(matcher);
                return request.getString("name").equals("Santiago") &&
                    request.getDouble("lat").equals(-34.397) &&
                    request.getDouble("lon").equals(150.644);
            })
        );
    }


}
