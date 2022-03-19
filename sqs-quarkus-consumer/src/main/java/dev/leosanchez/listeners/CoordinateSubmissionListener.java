package dev.leosanchez.listeners;

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import dev.leosanchez.qualifiers.ListenerQualifier;
import dev.leosanchez.services.CoordinatesService;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.json.JsonObject;


@ApplicationScoped
@RegisterForReflection
@ListenerQualifier(urlProperty = "oneway.queue.url")
public class CoordinateSubmissionListener implements IListener {

    @Inject
    CoordinatesService service;

    // listener for one way communication
    public Optional<String> process(String message) {
        JsonObject requestBody = new JsonObject(message);
        String city = requestBody.getString("name");
        Double lat = requestBody.getDouble("lat");
        Double lon = requestBody.getDouble("lon");
        service.addCoordinates(city, lat, lon);
        return Optional.empty();
    }

}
