package dev.leosanchez.producer.services;

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import dev.leosanchez.common.exceptions.MessageSendingException;
import io.vertx.core.json.JsonObject;

@ApplicationScoped
public class CoordinatesService {
    
    @ConfigProperty(name = "twoways.queue.url")
    String twoWaysQueueUrl;

    @ConfigProperty(name = "oneway.queue.url")
    String onewayResponseQueueUrl;

    @Inject
    QueueProducerService queueService;

    public Optional<JsonObject> queryCoordinates(String city) {
        // we build the request
        JsonObject request = new JsonObject();
        request.put("city", city);

        try {
        // we send the request and keep the signature
        String signature = queueService.sendMessageForResponse(twoWaysQueueUrl, request.toString());
        //we await the message just for 30 seconds
        Optional<String> response = queueService.receiveResponse(signature, 30);

        // we parse and return the response
        return response.isPresent()? Optional.of(new JsonObject(response.get())) : Optional.empty();
        } catch (MessageSendingException e) {
            return Optional.empty();
        }
    }

    public void submitCoordinates(String name, Double lat, Double lon) {
        JsonObject request = new JsonObject();
        request.put("name", name);
        request.put("lat", lat);
        request.put("lon", lon);
        try {
            queueService.sendMessageForNoResponse(onewayResponseQueueUrl, request.toString());
        } catch (MessageSendingException e) {
            e.printStackTrace();
        }
    }
}
