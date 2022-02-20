package dev.leosanchez;

import static org.mockito.ArgumentMatchers.argThat;

import javax.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import dev.leosanchez.providers.QueueClientProvider.SQSClientProvider;
import dev.leosanchez.services.CoordinatesService;
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
    SQSClientProvider queueProvider;

    // we implement some mock methods
    @BeforeEach
    public void setup() {
        JsonObject response = new JsonObject();
        response.put("lat", -34.397);
        response.put("lon", 150.644);

        // we configure some signature responses
        Mockito.when(queueProvider.sendMessage(
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
        Mockito.when(queueProvider.receiveResponse(
            argThat(matcher -> matcher.equals("CQBO") || matcher.equals("STGO")
        ), Mockito.anyInt())).thenAnswer(answer -> {
            if (answer.getArgument(0).equals("CQBO")) {
                return response.toString();
            } else {
                return null;
            }
        });
    }

    @Test
    public void testQueryCoordinates() {
        JsonObject response = service.queryCoordinates("Coquimbo");
        Assertions.assertEquals(response.getDouble("lat"), -34.397);
        Assertions.assertEquals(response.getDouble("lon"), 150.644);
    }

    @Test
    public void testNotFoundCoordinates() {
        JsonObject response = service.queryCoordinates("Santiago");
        Assertions.assertNull(response);
    }

    @Test
    public void testSubmitCoordinates() {
        service.submitCoordinates("Santiago", -34.397, 150.644);
        Mockito.verify(queueProvider, Mockito.times(1)).sendMessage(
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
