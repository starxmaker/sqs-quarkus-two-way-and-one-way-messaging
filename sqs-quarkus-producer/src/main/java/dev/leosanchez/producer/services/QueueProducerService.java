package dev.leosanchez.producer.services;

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

import dev.leosanchez.common.dto.QueueMessage;
import dev.leosanchez.common.adapters.queueadapter.IQueueAdapter;
import dev.leosanchez.common.exceptions.MessagePollingException;
import dev.leosanchez.common.exceptions.MessageRemovalException;
import dev.leosanchez.common.exceptions.MessageSendingException;

@ApplicationScoped
public class QueueProducerService {
    
    // just a logger
    private static final Logger LOG = Logger.getLogger(QueueProducerService.class);

    // the sdk client
    @Inject
    IQueueAdapter queueAdapter;

    // the name of the application to make queues with the same name as prefix
    // NOTE: the property is received as optional because it is not inserted in the
    // test profile and we want to test this class.
    @ConfigProperty(name = "quarkus.application.name")
    Optional<String> applicationName;

    // the response queue that will be created after the initialization of the class
    private Optional<String> responseQueueUrl = Optional.empty();

    // a stack that will receive messages for all the service, no matter the request made
    private Map<String, String> messageStack = new HashMap<>();

    // a variable that will be used to store the polling task in order to check if
    // it was done
    private Future<Void> pollingFuture;

    public void sendMessageForNoResponse(String targetQueueUrl, String message) throws MessageSendingException {
        LOG.info("Sending message " + message+" not expecting response");
        queueAdapter.sendMessage(targetQueueUrl, message);
    }

    public Optional<String> getResponseQueueUrl () {
        return responseQueueUrl;
    }

    public String sendMessageForResponse(String targetQueueUrl, String message) throws MessageSendingException {
        // we generate a signature
        String signature = UUID.randomUUID().toString();
        LOG.info("Sending message " + message+" expecting response");
        // we assign the attributes to the message
        Map<String, String> messageAttributes = new HashMap<>() {
            {
                put("ResponseQueueUrl", retrieveResponseQueueUrl().get()); // we make sure that it is initialized
                // we attach the generated signature to the message
                put("Signature", signature);
            }
        };
        // we send the message through our adapter
        queueAdapter.sendMessageWithAttributes(targetQueueUrl, message, messageAttributes);
        // we return the generated signature
        return signature;
    }

    public Optional<String> receiveResponse(String signature, Integer secondsToTimeout)  {
        LOG.info("Awaiting response");
        // we poll for the messages in another thread
        ExecutorService waiterExecutor = Executors.newSingleThreadExecutor();
        Optional<String> receivedMessage = Optional.empty(); // if timeout, it will return null
        try {
            // we create a future that will wait for the response
            CompletableFuture<Optional<String>> future = CompletableFuture.supplyAsync(() -> {
                Optional<String> response = findMessage(signature);
                while (response.isEmpty()) {
                    LOG.info("Message not found, polling");
                    // if the variable that contains the polling task is not null and it is not done, then wait
                    if (Objects.nonNull(pollingFuture) && !pollingFuture.isDone()) {
                        LOG.info("There is already a polling in progress, so waiting");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            return Optional.empty();
                        }
                    } else {
                        // if the variable is null or it is done, then we start a new polling task
                        LOG.info("A new polling will be executed");
                        pollingFuture = CompletableFuture.runAsync(() -> {
                            try {
                                pollMessages();
                            } catch (MessagePollingException e) {
                                throw new RuntimeException(e);
                            }
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
        try {
            LOG.info("Initializing response queue");
            String projectName = applicationName.orElse("TEST");
            // we define a prefix for the generated response queues (Warning: queues cannot
            // have a name with a length with more than 80 characters)
            String prefix = projectName + "_RQ_TEMP_";
            // we create a unique name for the response queue
            String queueName = prefix + UUID.randomUUID().toString();
            // we receive the queue url
            responseQueueUrl = Optional.of(queueAdapter.createQueue(queueName));
        } catch (Exception e){
            LOG.error("Error creating response queue");
            e.printStackTrace();
            responseQueueUrl = Optional.empty();
        }
    }

    private void pollMessages() throws MessagePollingException {
        LOG.info("Polling messages");
        // we prepare the request
        List<QueueMessage> messages = queueAdapter.receiveMessages(responseQueueUrl.get(), 10);
        if (messages.size() > 0) {
            LOG.info("Messages received");
            for (QueueMessage message : messages) {
                Map<String, String> attributes = message.getAttributes();
                String signature = attributes.get("Signature");
                if (Objects.nonNull(signature)) {
                    messageStack.put(signature, message.getMessage());
                }
                try{
                    // we remove it from the queue
                    queueAdapter.deleteMessage(responseQueueUrl.get(), message.getReceiptHandle());
                } catch (MessageRemovalException e) {
                    LOG.error("Error removing message");
                    e.printStackTrace();
                }
            }
        } else {
            LOG.info("No messages");
        }

    }



    private Optional<String> findMessage(String signature) {
        LOG.info("Finding message");
        String response = messageStack.get(signature);
        if (Objects.nonNull(response)) {
            // if there is a message with the signature, we remove it from the list and we
            // return it
            messageStack.remove(signature);
            return Optional.of(response);
        }
        return Optional.empty();
    }


    private Optional<String> retrieveResponseQueueUrl() {
        LOG.info("Retrieving current response queue");
        // if the value is null, lets wait until it is initialized
        while (responseQueueUrl.isEmpty()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (responseQueueUrl.isEmpty()) {
            throw new Error("No response queue created");
        } else {
            LOG.infov("response queue created {0}", responseQueueUrl.get());
        }
        // so here we return the value only when we know it is initialized
        return responseQueueUrl;
    }

    @PreDestroy
    public void deleteResponseQueue() {
        try {
            LOG.info("Deleting queue: " + responseQueueUrl);
            queueAdapter.deleteQueue(responseQueueUrl.get());
        } catch (Exception e) {
            LOG.error("Error while deleting queue", e);
        }
    }
    
}
