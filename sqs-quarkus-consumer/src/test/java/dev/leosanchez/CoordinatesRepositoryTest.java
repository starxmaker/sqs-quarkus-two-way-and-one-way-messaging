package dev.leosanchez;


import java.util.List;

import javax.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import dev.leosanchez.repositories.CoordinatesRepository;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class CoordinatesRepositoryTest {
    
    @Inject
    CoordinatesRepository repository;

    @Test
    public void testGetCoordinates() {
        List<Double> coordinates = repository.getCoordinates("Santiago");
        Assertions.assertNotNull(coordinates);
        Assertions.assertEquals(-33.447487, coordinates.get(0));
        Assertions.assertEquals(-70.673676, coordinates.get(1));
    }

    @Test
    public void testNotFoundCoordinates() {
        List<Double> coordinates = repository.getCoordinates("NotFound");
        Assertions.assertNull(coordinates);
    }

    @Test
    public void testCoordinatesInsertion() {
        repository.addCoordinates("Punta Arenas", -53.7873884,-53.7873884);
        List<Double> coordinates = repository.getCoordinates("Punta Arenas");
        Assertions.assertNotNull(coordinates);
        Assertions.assertEquals(-53.7873884, coordinates.get(0));
        Assertions.assertEquals(-53.7873884, coordinates.get(1));
    }
    

}
