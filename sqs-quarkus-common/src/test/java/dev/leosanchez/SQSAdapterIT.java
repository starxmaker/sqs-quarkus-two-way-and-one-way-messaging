package dev.leosanchez;

import dev.leosanchez.common.adapters.queueadapter.SQSAdapter;
import dev.leosanchez.common.dto.QueueMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.time.Duration;

@QuarkusTest
@TestProfile(SQSAdapterIT.TestProfile.class)
public class SQSAdapterIT {
        @Inject
        SQSAdapter adapter;

        public static class TestProfile implements QuarkusTestProfile {
                @Override
                public Map<String, String> getConfigOverrides() {
                        SQSAdapterIT.localstack.start();
                        String containerUrl = "http://" + localstack.getHost() + ":" + localstack.getFirstMappedPort();
                        return new HashMap<String, String>() {
                                {
                                        put("queue.provider", "sqs");
                                        put("quarkus.sqs.endpoint-override", containerUrl);
                                        put("quarkus.sqs.aws.region", "us-east-1");
                                        put("quarkus.sqs.aws.credentials.type", "static");
                                        put("quarkus.sqs.aws.credentials.static-provider.access-key-id", "AAEEII");
                                        put("quarkus.sqs.aws.credentials.static-provider.secret-access-key", "AAEEII");
                                }
                        };

                }
        }

        @Container
        public static GenericContainer<?> localstack = new GenericContainer<>(
                        DockerImageName.parse("localstack/localstack:0.11.1"))
                        .withEnv(new HashMap<String, String>() {
                                {
                                        put("SERVICES", "sqs");
                                        put("START_WEB", "0");
                                }
                        })
                        .withExposedPorts(4566).waitingFor(
                                        Wait.forLogMessage(".*Ready.*\\n", 1))
                        .withStartupTimeout(Duration.ofSeconds(180));

        @ConfigProperty(name = "quarkus.sqs.endpoint-override")
        String containerUrl;

        @Test
        public void testCreateQueue() {
                try {
                        adapter.createQueue("test");
                        Optional<String> queueUrl = adapter.getQueueUrl("test");
                        Assertions.assertTrue(queueUrl.isPresent());
                } catch (Exception e) {
                        Assertions.fail(e.getMessage());
                }

        }

        @Test
        public void testDeleteQueue() {
                try {
                        adapter.createQueue("testDelete");
                        Optional<String> queueUrlBeforeRemoval = adapter.getQueueUrl("testDelete");
                        adapter.deleteQueue(queueUrlBeforeRemoval.get());
                        Optional<String> queueUrlAfterRemoval = adapter.getQueueUrl("testDelete");
                        Assertions.assertFalse(queueUrlAfterRemoval.isPresent());
                } catch (Exception e) {
                        Assertions.fail(e.getMessage());

                }
        }

        @Test
        public void testSendAndReceiveMessage() {
                try {
                        adapter.createQueue("testSendMessage");
                        Optional<String> queueUrl = adapter.getQueueUrl("testSendMessage");
                        adapter.sendMessageWithAttributes(queueUrl.get(), "test", Map.of("key", "value"));
                        List<QueueMessage> messages = adapter.receiveMessages(queueUrl.get(), 1);
                        Assertions.assertEquals(1, messages.size());
                        Assertions.assertEquals("test", messages.get(0).getMessage());
                        Assertions.assertEquals("value", messages.get(0).getAttributes().get("key"));
                } catch (Exception e) {
                        Assertions.fail(e.getMessage());
                }
        }

        @Test
        public void testDeleteMessage() {
                try {
                        adapter.createQueue("testDeleteMessage");
                        Optional<String> queueUrl = adapter.getQueueUrl("testDeleteMessage");
                        adapter.sendMessage(queueUrl.get(), "test");
                        List<QueueMessage> messages = adapter.receiveMessages(queueUrl.get(), 1);
                        Assertions.assertEquals(1, messages.size());
                        adapter.deleteMessage(queueUrl.get(), messages.get(0).getReceiptHandle());
                        messages = adapter.receiveMessages(queueUrl.get(), 1);
                        Assertions.assertEquals(0, messages.size());
                } catch (Exception e) {
                        Assertions.fail(e.getMessage());
                }

        }

}
