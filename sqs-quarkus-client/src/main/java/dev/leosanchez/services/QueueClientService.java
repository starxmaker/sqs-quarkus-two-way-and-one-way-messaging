package dev.leosanchez.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import dev.leosanchez.DTO.QueueMessage;
import dev.leosanchez.adapters.QueueClientAdapter.IQueueClientAdapter;

@ApplicationScoped
public class QueueService {
    
    // just a logger
    private static final Logger LOG = Logger.getLogger(QueueService.class);

    // the sdk client
    @Inject
    IQueueClientAdapter queueAdapter;

    // the name of the application to make queues with the same name as prefix
    // NOTE: the property is received as optional because it is not inserted in the
    // test profile and we want to test this class.
    @ConfigProperty(name = "quarkus.application.name")
    Optional<String> applicationName;

    // the response queue that will be created after the initialization of the class
    private String responseQueueUrl;

    // a stack that will receive messages for all the service, no matter the request made
    private Map<String, String> messageStack = new HashMap<>();

    // a variable that will be used to store the polling task in order to check if
    // it was done
    private Future<Void> pollingFuture;

    public String sendMessage(String targetQueueUrl, String message) {
        // we generate a signature
        String signature = UUID.randomUUID().toString();
        LOG.info("Sending message " + message);
        // we assign the attributes to the message
        Map<String, String> messageAttributes = new HashMap<>() {
            {
                put("ResponseQueueUrl", retrieveResponseQueue()); // we make sure that it is initialized
                // we attach the generated signature to the message
                put("Signature", signature);
            }
        };
        // we send the message through our adapter
        queueAdapter.sendMessage(targetQueueUrl, message, messageAttributes);
        // we return the generated signature
        return signature;
    }

    public String receiveResponse(String signature, Integer secondsToTimeout) {
        LOG.info("Awaiting response");
        // we poll for the messages in another thread
        ExecutorService waiterExecutor = Executors.newSingleThreadExecutor();
        String receivedMessage = null; // if timeout, it will return null
        try {
            // we create a future that will wait for the response
            CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
                String response = findMessage(signature);
                while (Objects.isNull(response)) {
                    LOG.info("Message not found, polling");
                    // if the variable that contains the polling task is not null and it is not done, then wait
                    if (Objects.nonNull(pollingFuture) && !pollingFuture.isDone()) {
                        LOG.info("There is already a polling in progress, so waiting");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            return null;
                        }
                    } else {
                        // if the variable is null or it is done, then we start a new polling task
                        LOG.info("A new polling will be executed");
                        pollingFuture = CompletableFuture.runAsync(() -> {
                            pollMessages();
                        });
                    }
                    response = findMessage(signature);
                }
                return response;
            }, waiterExecutor);
            receivedMessage = future.get(secondsToTimeout, TimeUnit.SECONDS); // here we wait for the response
        } catch (Exception e) {
            // if there is an error, we print the stacktrace
            LOG.error("Timeout");
            e.printStackTrace();
        } finally {
            // we terminate the thread created
            waiterExecutor.shutdownNow();
        }
        // we return the received message or null if error
        return receivedMessage;
    }

    @PostConstruct // we make sure this is executed after the initialization of the class
    public void createResponseQueue() {
        LOG.info("Initializing response queue");
        String projectName = applicationName.orElse("TEST");
        // we define a prefix for the generated response queues (Warning: queues cannot
        // have a name with a length with more than 80 characters)
        String prefix = projectName + "_RQ_TEMP_";
        // we create a unique name for the response queue
        String responseQueueName = prefix + UUID.randomUUID().toString();
        // we receive the queue url
        responseQueueUrl = queueAdapter.createQueue(responseQueueName);
    }

    private void pollMessages() {
        LOG.info("Polling messages");
        // we prepare the request
        List<QueueMessage> messages = queueAdapter.receiveMessages(responseQueueUrl, 10);
        if (messages.size() > 0) {
            LOG.info("Messages received");
            for (QueueMessage message : messages) {
                Map<String, String> attributes = message.getAttributes();
                String signature = attributes.get("Signature");
                if (Objects.nonNull(signature)) {
                    messageStack.put(signature, message.getMessage());
                }
                // we remove it from the queue
                queueAdapter.deleteMessage(responseQueueUrl, message.getReceiptHandle());
            }
        } else {
            LOG.info("No messages");
        }

    }

    @PreDestroy
    public void deleteResponseQueue() {
        try {
            LOG.info("Deleting queue: " + responseQueueUrl);
            queueAdapter.deleteQueue(responseQueueUrl);
        } catch (Exception e) {
            LOG.error("Error while deleting queue", e);
        }
    }


    private String findMessage(String signature) {
        LOG.info("Finding message");
        String response = messageStack.get(signature);
        if (Objects.nonNull(response)) {
            // if there is a message with the signature, we remove it from the list and we
            // return it
            messageStack.remove(signature);
        }
        return response;
    }


    private String retrieveResponseQueue() {
        LOG.info("Retrieving current response queue");
        // if the value is null, lets wait until it is initialized
        while (Objects.isNull(responseQueueUrl)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (responseQueueUrl.equals("NO_QUEUE_CREATED")) {
            throw new Error("No response queue created");
        }
        // so here we return the value only when we know it is initialized
        return responseQueueUrl;
    }
}
