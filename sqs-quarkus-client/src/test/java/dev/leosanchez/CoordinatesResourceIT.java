package dev.leosanchez;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.json.JsonObject;

import static io.restassured.RestAssured.given;

import javax.ws.rs.core.MediaType;

@QuarkusTest
public class CoordinatesResourceIT {

    @Test
    public void testQueryEndpoint() {
        String rawMessage = given()
                .when().get("/coordinates/search/{query}", "Santiago")
                .then()
                .statusCode(200)
                .extract()
                .asString();
        JsonObject messageReceived = new JsonObject(rawMessage);
        Assertions.assertNotNull(messageReceived.getDouble("lat"));
        Assertions.assertNotNull(messageReceived.getDouble("lon"));
    }

    @Test
    public void testQueryEndpointDelays() {
        Long startTime = System.currentTimeMillis();
        for (int i = 0; i < 3 ; i++) {
            given()
                .when().get("/coordinates/search/{query}", "Santiago")
                .then()
                .statusCode(200);
        }
        Long endTime = System.currentTimeMillis();
        // time between these 3 calls should be at least 10 seconds
        Assertions.assertTrue(endTime - startTime > 10000);
        
    }

    @Test
    public void testSubmitEndpoint() {
        JsonObject body = new JsonObject();
        body.put("name", "Coquimbo");
        body.put("lat", -30.4);
        body.put("lon", -71.5);

        given()
          .when()
          .contentType(MediaType.APPLICATION_JSON)
          .body( body.encode())
          .post("/coordinates/submit")
          .then()
             .statusCode(200);
    }
}
