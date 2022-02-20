package dev.leosanchez.services;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import dev.leosanchez.providers.QueueClientProvider.SQSClientProvider;
import io.vertx.core.json.JsonObject;

@ApplicationScoped
public class CoordinatesService {
    
    @ConfigProperty(name = "twoways.queue.url")
    String twoWaysQueueUrl;

    @ConfigProperty(name = "oneway.queue.url")
    String onewayResponseQueueUrl;

    @Inject
    SQSClientProvider queueProvider;

    public JsonObject queryCoordinates(String city) {
        // we build the request
        JsonObject request = new JsonObject();
        request.put("city", city);

        // we send the request and keep the signature
        String signature = queueProvider.sendMessage(twoWaysQueueUrl, request.toString());
        //we await the message just for 30 seconds
        String response = queueProvider.receiveResponse(signature, 30);

        // we parse and return the response
        return response!=null? new JsonObject(response) : null;
    }

    public void submitCoordinates(String name, Double lat, Double lon) {
        JsonObject request = new JsonObject();
        request.put("name", name);
        request.put("lat", lat);
        request.put("lon", lon);

        queueProvider.sendMessage(onewayResponseQueueUrl, request.toString());
    }
}
