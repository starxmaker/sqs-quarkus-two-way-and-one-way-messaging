package dev.leosanchez.listeners;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import dev.leosanchez.annotations.Listen;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.json.JsonObject;


@RegisterForReflection
public class CoordinatesListener {

    private Map<String, Long> lastQueryCalls = new HashMap<>();

    // listener for two ways comunication
    @Listen( urlProperty = "twoways.queue.url", parallelProcessing = false)
    public String listen(String message) throws InterruptedException {
        ensureTimeBetweenCalls("listen", 5000);
        JsonObject requestBody = new JsonObject(message);
        String city = requestBody.getString("city");
        JsonObject json = new JsonObject();
        json.put("name", city);
        json.put("lat", -41.5);
        json.put("lon", -72.9);
        return json.encode();

    }

    // listener for one way communication
    @Listen( urlProperty = "oneway.queue.url") 
    public void listenAsync(String message) {
        System.out.println("we listened "+message);
    }


    // very rudimental method to ensure that the listener is not called too often
    private void ensureTimeBetweenCalls(String methodName, Integer delayTime) {
        try {
            Long lastQueryCall = lastQueryCalls.get(methodName);
            Long currentTime = System.currentTimeMillis();
            if(Objects.nonNull(lastQueryCall) && currentTime - lastQueryCall < delayTime) {
                    Thread.sleep(delayTime - (currentTime - lastQueryCall));
                    currentTime = System.currentTimeMillis();
            } 
            lastQueryCalls.put(methodName, currentTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
