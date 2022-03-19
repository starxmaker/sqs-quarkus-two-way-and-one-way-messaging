package dev.leosanchez;


import java.util.List;

import javax.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import dev.leosanchez.repositories.CoordinatesRepository;
import dev.leosanchez.services.CoordinatesService;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class CoordinatesServiceTest {
    
    @Inject
    CoordinatesService service;

    @Test
    public void testGetCoordinates() {
        List<Double> coordinates = service.getCoordinates("Santiago");
        Assertions.assertNotNull(coordinates);
        Assertions.assertEquals(-33.447487, coordinates.get(0));
        Assertions.assertEquals(-70.673676, coordinates.get(1));
    }

    @Test
    public void testNotFoundCoordinates() {
        List<Double> coordinates = service.getCoordinates("NotFound");
        Assertions.assertNull(coordinates);
    }

    @Test
    public void testCoordinatesInsertion() {
        service.addCoordinates("Punta Arenas", -53.7873884,-53.7873884);
        List<Double> coordinates = service.getCoordinates("Punta Arenas");
        Assertions.assertNotNull(coordinates);
        Assertions.assertEquals(-53.7873884, coordinates.get(0));
        Assertions.assertEquals(-53.7873884, coordinates.get(1));
    }
    

}
