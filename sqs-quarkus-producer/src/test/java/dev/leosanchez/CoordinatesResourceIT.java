package dev.leosanchez;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.json.JsonObject;

import static io.restassured.RestAssured.given;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import java.time.Duration;

@QuarkusTest
@TestProfile(CoordinatesResourceIT.TestProfile.class)
public class CoordinatesResourceIT {

    @Container
    public static GenericContainer<?> localstack = new GenericContainer<>(
            DockerImageName.parse("localstack/localstack:0.11.1"))
            .withEnv(new HashMap<String, String>() {
                {
                    put("SERVICES", "sqs");
                    put("START_WEB", "0");
                    put("DEFAULT_REGION", "us-east-1");
                }
            })
            .withExposedPorts(4566).waitingFor(
                    Wait.forLogMessage(".*Ready.*\\n", 1))
            .withStartupTimeout(Duration.ofSeconds(180));
    public static GenericContainer<?> consumerContainer = new GenericContainer<>(
            DockerImageName.parse("leonelsanchez/sqs-quarkus-consumer:1.0.0-SNAPSHOT"))
            .withExposedPorts(8080).waitingFor(
                    Wait.forLogMessage(".*Listening on.*\\n", 1))
            .withStartupTimeout(Duration.ofSeconds(180));

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            // localstack configuration
            Network network = Network.newNetwork();
            localstack.withNetwork(network);
            localstack.withNetworkAliases("localstack");
            localstack.start();
            try {
                localstack.execInContainer("awslocal", "sqs", "create-queue", "--queue-name", "OneWayQueue");
                localstack.execInContainer("awslocal", "sqs", "create-queue", "--queue-name", "TwoWaysQueue");
            } catch (Exception e) {
                e.printStackTrace();
            }
            String localstackInternalUrl = "http://localstack:4566";
            // consumer configuration
            consumerContainer.withEnv("twoways.queue.url", localstackInternalUrl + "/queue/TwoWaysQueue");
            consumerContainer.withEnv("oneway.queue.url", localstackInternalUrl + "/queue/OneWayQueue");
            consumerContainer.withEnv("quarkus.sqs.endpoint-override", localstackInternalUrl);
            consumerContainer.withNetwork(network);
            consumerContainer.start();
            // producer configuration
            String localstackUrl = "http://" + localstack.getHost() + ":" + localstack.getFirstMappedPort();
            return new HashMap<String, String>() {
                {
                    put("twoways.queue.url", localstackUrl + "/queue/TwoWaysQueue");
                    put("oneway.queue.url", localstackUrl + "/queue/OneWayQueue");
                    put("queue.provider", "sqs");
                    put("quarkus.sqs.endpoint-override", localstackUrl);
                    put("quarkus.sqs.aws.region", "us-east-1");
                    put("quarkus.sqs.aws.credentials.type", "static");
                    put("quarkus.sqs.aws.credentials.static-provider.access-key-id", "AAEEII");
                    put("quarkus.sqs.aws.credentials.static-provider.secret-access-key", "AAEEII");
                }
            };

        }
    }

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
    public void testSubmitEndpoint() throws InterruptedException {
        // we send a request to submit coordinates of a new city
        JsonObject body = new JsonObject();
        body.put("name", "Portsmouth");
        body.put("lat", 50.8047148);
        body.put("lon", -1.1667698);

        given()
                .when()
                .contentType(MediaType.APPLICATION_JSON)
                .body(body.encode())
                .post("/coordinates/submit")
                .then()
                .statusCode(200);

        // we wait for a reasonable time
        Thread.sleep(5000);

        // we check if the city was inserted
        String rawMessage = given()
                .when().get("/coordinates/search/{query}", "Portsmouth")
                .then()
                .statusCode(200)
                .extract()
                .asString();
        JsonObject messageReceived = new JsonObject(rawMessage);
        Assertions.assertNotNull(messageReceived.getDouble("lat"));
        Assertions.assertNotNull(messageReceived.getDouble("lon"));

    }
}
