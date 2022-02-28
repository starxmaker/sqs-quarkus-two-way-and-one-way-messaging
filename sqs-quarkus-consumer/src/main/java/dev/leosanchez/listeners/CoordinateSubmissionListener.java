package dev.leosanchez.listeners;

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import dev.leosanchez.repositories.CoordinatesRepository;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.json.JsonObject;


@ApplicationScoped
@RegisterForReflection
@ListenerQualifier(urlProperty = "oneway.queue.url")
public class CoordinateSubmissionListener implements IListener {


    @Inject
    CoordinatesRepository repository;

    // listener for one way communication
    public Optional<String> process(String message) {
        JsonObject requestBody = new JsonObject(message);
        String city = requestBody.getString("name");
        Double lat = requestBody.getDouble("lat");
        Double lon = requestBody.getDouble("lon");
        repository.addCoordinates(city, lat, lon);
        return Optional.empty();
    }

}
