package dev.leosanchez.listeners;

import java.util.List;
import java.util.Objects;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;


import org.eclipse.microprofile.config.inject.ConfigProperty;

import dev.leosanchez.DTO.PollingRequest.PollingRequestBuilder;
import dev.leosanchez.providers.QueueConsumerProvider.IQueueConsumerProvider;
import dev.leosanchez.providers.QueueConsumerProvider.SQSConsumerProvider;
import dev.leosanchez.repositories.CoordinatesRepository;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.configuration.ProfileManager;
import io.vertx.core.json.JsonObject;


@Startup
@ApplicationScoped
public class CoordinatesListener {


    @Inject
    CoordinatesRepository repository;

    @Inject
    IQueueConsumerProvider provider;

    @ConfigProperty(name = "twoways.queue.url")
    String twoWaysQueueUrl;

    @ConfigProperty(name = "oneway.queue.url")
    String onewayQueueUrl;

    
    // listener for two ways comunication
    public String listen(String message){
        try {
            JsonObject requestBody = new JsonObject(message);
            String city = requestBody.getString("city");
            List<Double> coordinates = repository.getCoordinates(city);
            JsonObject json = new JsonObject();
            json.put("name", city);
            if(Objects.nonNull(coordinates)) {
                json.put("lat", coordinates.get(0));
                json.put("lon", coordinates.get(1));
            }
            return json.encode();
        } catch(Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // listener for one way communication
    public String listenAsync(String message) {
        JsonObject requestBody = new JsonObject(message);
        String city = requestBody.getString("name");
        Double lat = requestBody.getDouble("lat");
        Double lon = requestBody.getDouble("lon");
        repository.addCoordinates(city, lat, lon);
        return null;
    }

    @PostConstruct
    public void startListeners() {
        if(!ProfileManager.getActiveProfile().equals("test")) {
            // here we declare each listener
            provider.listen(
                PollingRequestBuilder.builder()
                    .queueUrl(twoWaysQueueUrl)
                    .processer(this::listen)
                    .noParallelProcessing()
                    .minExecutionTime(20)
                .build()
            );
            provider.listen(
                PollingRequestBuilder.builder()
                    .queueUrl(onewayQueueUrl)
                    .processer(this::listenAsync)
                .build()
            );
        }
    }
}
