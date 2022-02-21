package dev.leosanchez.repositories;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CoordinatesRepository {
    private Map<String, List<Double>> coordinates = new HashMap<>() {{
        put("Santiago", List.of(-33.447487, -70.673676));
        put("Coquimbo", List.of(-30.657041, -71.8844573));
    }};

    public List<Double> getCoordinates(String city) {
        return coordinates.get(city);
    }

    public void addCoordinates(String city, double lat, double lon) {
        coordinates.put(city, List.of(lat, lon));
    }
}
