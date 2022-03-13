package dev.leosanchez.listeners;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import dev.leosanchez.repositories.CoordinatesRepository;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.json.JsonObject;


@ApplicationScoped
@RegisterForReflection
@ListenerQualifier(urlProperty = "twoways.queue.url", parallelProcessing = false, minProcessingMilliseconds = 20)
public class CoordinateQueryListener  implements IListener {

    @Inject
    CoordinatesRepository repository;
    
    // listener for two ways comunication
    public Optional<String> process(String message){
        try {
            // convert the original message to json
            JsonObject requestBody = new JsonObject(message);
            // extract the city name to be searched
            String city = requestBody.getString("city");
            // we make the query
            List<Double> coordinates = repository.getCoordinates(city);
            // we start building the response
            JsonObject json = new JsonObject();
            if(Objects.nonNull(coordinates)) {
                // coordinates found
                json.put("name", city);
                json.put("lat", coordinates.get(0));
                json.put("lon", coordinates.get(1));
                json.put("status", "OK");
            } else {
                // coordinates not found
                json.put("status", "NO_RESULTS");
            }
            return Optional.of(json.encode());
        } catch(Exception e) {
            // any error
            e.printStackTrace();
            JsonObject json = new JsonObject(); 
            json.put("status", "INTERNAL_SERVER_ERROR" );
            return Optional.of(json.encode());
        }
    }
}
