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
    public void testQueryEndpointNotFound() {
        String rawMessage = given()
                .when().get("/coordinates/search/{query}", "Chuchuncocity")
                .then()
                .statusCode(200)
                .extract()
                .asString();
        JsonObject messageReceived = new JsonObject(rawMessage);
        Assertions.assertNull(messageReceived.getDouble("lat"));
        Assertions.assertNull(messageReceived.getDouble("lon"));
        Assertions.assertEquals("NO_RESULTS", messageReceived.getString("status"));
        
    }

    @Test
    public void testSubmitEndpoint() throws InterruptedException{
        // we send a request to submit coordinates of a new city
        JsonObject body = new JsonObject();
        body.put("name", "Puerto Natales");
        body.put("lat", -51.7293526);
        body.put("lon", -72.5283157);

        given()
          .when()
          .contentType(MediaType.APPLICATION_JSON)
          .body( body.encode())
          .post("/coordinates/submit")
          .then()
             .statusCode(200);

        // we wait for a reasonable time
        Thread.sleep(5000);

        // we check if the city was inserted
        String rawMessage = given()
                .when().get("/coordinates/search/{query}", "Puerto Natales")
                .then()
                .statusCode(200)
                .extract()
                .asString();
        JsonObject messageReceived = new JsonObject(rawMessage);
        Assertions.assertNotNull(messageReceived.getDouble("lat"));
        Assertions.assertNotNull(messageReceived.getDouble("lon"));

    }
}
