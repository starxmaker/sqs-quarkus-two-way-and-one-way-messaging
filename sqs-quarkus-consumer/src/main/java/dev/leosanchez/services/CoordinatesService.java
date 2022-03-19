package dev.leosanchez.services;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import dev.leosanchez.repositories.CoordinatesRepository;

@ApplicationScoped
public class CoordinatesService {
    @Inject
    CoordinatesRepository repository;
    
    public List<Double> getCoordinates(String city) {
        return repository.getCoordinates(city);
    }
    
    public void addCoordinates(String city, double lat, double lon) {
        repository.addCoordinates(city, lat, lon);
    }
}
