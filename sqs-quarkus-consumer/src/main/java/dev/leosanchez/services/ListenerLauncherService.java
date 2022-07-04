package dev.leosanchez.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import dev.leosanchez.common.dto.QueueMessage;
import dev.leosanchez.common.exceptions.MessagePollingException;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import dev.leosanchez.DTO.ListenRequest;
import dev.leosanchez.listeners.IListener;
import dev.leosanchez.qualifiers.ListenerQualifier;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.configuration.ProfileManager;

@ApplicationScoped
@Startup
public class ListenerLauncherService {

    // a simple logger
    private static Logger LOG = Logger.getLogger(ListenerLauncherService.class);

    // our listeners injected and filtered by the qualifier
    @ListenerQualifier
    Instance<IListener> partialListeners;

    // the provider we implemented
    @Inject
    QueueConsumerService queueConsumerService;

    @PostConstruct
    public void init() {
        // we just want to launch the listeners if the profile is not test
        if (!ProfileManager.getActiveProfile().equals("test")) {
            LOG.info("Launching listeners");
            // we transform the data so we can handle it in a more readable way
            List<ListenRequest> requests = extractListenRequests();
            // we launch the listening orchestation in a different thread to avoid blocking the main thread
            Executors.newSingleThreadExecutor().submit(() -> orchestrateListeners(requests, null));
        }
    }

    private List<ListenRequest> extractListenRequests() {
        // our initial response
        List<ListenRequest> requests = new ArrayList<>();
        // we iterate the injected listeners
        for (IListener listener : partialListeners) {
            // we extract the original class name from the proxy (Quarkus does not inject the bean directly)
            String listenerProxyClassName = listener.getClass().getName();
            String listenerClassName = cleanClassName(listenerProxyClassName);
            // we load the original class
            Class<?> listenerClass = null;
            try {
                listenerClass = Class.forName(listenerClassName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                LOG.error("Metadata for listener " + listenerClassName + " not found. Skipping...");
                continue;
            }
            // We get the annotation from the original class
            ListenerQualifier annotation = listenerClass.getAnnotation(ListenerQualifier.class);
            // if the annotation has an valid url property we continue
            if (Objects.nonNull(annotation.urlProperty()) && !annotation.urlProperty().equals("")) {
                // we get the url from properties
                String url = ConfigProvider.getConfig().getValue(annotation.urlProperty(), String.class);
                // we build an object containing all the information
                ListenRequest lr = new ListenRequest(listener, url, annotation.parallelProcessing(),
                        annotation.maxNumberOfMessagesPerProcessing(), annotation.minProcessingMilliseconds());
                // we append it to our response
                requests.add(lr);
            }
        }
        return requests;
    }

    public void orchestrateListeners(List<ListenRequest> requests, Integer pollingQuantity) {
        // here we will store the current executions
        Map<String, Future<?>> currentExecutions = new HashMap<>();
        // we will also keep a record of the quantity of the pollings performed per listener
        Map<String, Integer> pollingRecord = requests.stream().collect(Collectors.toMap(ListenRequest::getQueueUrl, e -> 0));
        // we keep a record of suspensions in case a polling fails
        Map<String, Long> queuePollingSuspension = new HashMap<>();
        Long failingSuspension = 5 * 60 * 1000L;
        
        // iterate continuosly  or until iterations are done
        while (Objects.isNull(pollingQuantity) || !pollingRecord.values().stream().allMatch(p -> p >= pollingQuantity)) {
            // we iterate each request extracted
            for (ListenRequest request : requests) {
                // we check how much pollings have been done for this request
                Integer currentIterations = pollingRecord.get(request.getQueueUrl());
                if (! queuePollingSuspension.containsKey(request.getQueueUrl()) ||  queuePollingSuspension.get(request.getQueueUrl()) < System.currentTimeMillis()) {
                    // if we dont limit the number of pollings or  if the current number of pollings is less than the one desired, continue
                    if (Objects.isNull(pollingQuantity) || currentIterations <  pollingQuantity ) {
                        // we verify if there is a current execution for this request
                        Future<?> currentTask = currentExecutions.get(request.getQueueUrl());
                        // if there is no execution or if the current execution is done, run a new one for the request
                        // if there is an execution not done, we skip this request and in a new execution we will check if it is finished
                        if (Objects.isNull(currentTask) || currentTask.isDone()) {
                            // we define and run the new task
                            Future<?> currentExecution = CompletableFuture.runAsync(() -> {
                                try {
                                    performPolling(request);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    LOG.error("Polling for " + request.getQueueUrl() + " failed, retrying in "+failingSuspension+" milliseconds");
                                    queuePollingSuspension.put(request.getQueueUrl(), System.currentTimeMillis() + failingSuspension);
                                }
                            });
                            // we save it on our records
                            currentExecutions.put(request.getQueueUrl(), currentExecution);
                            // if the polling quantity param was specified, then update the polling records
                            if(Objects.nonNull(pollingQuantity)) {
                                pollingRecord.put(request.getQueueUrl(), currentIterations + 1);
                            }
                        }
                    }
                }
            }
        }
        // once the polling limit is reached, we wait for the current executions to finish
        for (Future<?> future : currentExecutions.values()) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private void performPolling(ListenRequest request) throws MessagePollingException {
            LOG.info("polling messages for queue " + request.getQueueUrl());
            // we poll messages from the queue
            List<QueueMessage> messages = queueConsumerService.pollMessages(request.getQueueUrl(),
                    request.getMaxMessagesPerPolling());
            if (messages.isEmpty()) {
                LOG.info("No messages received for queue" + request.getQueueUrl());
            } else {
                // if we receive a message, we start processing
                LOG.info("Received " + messages.size() + " messages");
                // we configure a consumer for the messages we receive
                Consumer<QueueMessage> consumer = message -> {
                    onMessage(message, request.getListener(), request.getMinExecutionMilliseconds());
                };
                // if we configured parallel processing, we use it
                if (request.isParallelProcessing()) {
                    messages.parallelStream().forEach(consumer);
                } else {
                    // if not, the messages will be processed sequentially
                    messages.stream().forEach(consumer);
                }
            }
        
    }

    private void onMessage(QueueMessage message, IListener listener, int minProcessingMilliseconds) {
        Long startExecution = System.currentTimeMillis();
        // we invoke the method
        Optional<String> response = listener.process(message.getMessage());
        // if the response was not null we send it to the source queue according to its signature
        if (response.isPresent()) {
            String sourceQueueUrl = message.getAttributes().get("ResponseQueueUrl");
            String signature = message.getAttributes().get("Signature");
            if (Objects.nonNull(sourceQueueUrl) && Objects.nonNull(signature)) {
                try {
                    queueConsumerService.sendAnswer(sourceQueueUrl, response.get(), signature);
                } catch (Exception e) {
                    LOG.error("Error sending message");
                    e.printStackTrace();
                }
            } else {
                LOG.error("ResponseQueueUrl or Signature not found in message attributes");
            }
        }
        // if the execution time was lower than the min expected, sleep
        Long currentTime = System.currentTimeMillis();
        if (currentTime - startExecution < minProcessingMilliseconds) {
            LOG.infov("Waiting for {0} ms",
                    minProcessingMilliseconds - (currentTime - startExecution));
            try {
                Thread.sleep(minProcessingMilliseconds - (currentTime - startExecution));
            } catch (InterruptedException e) {
                LOG.error("Interrupted while waiting for minimum execution time");
                e.printStackTrace();
            }
        }
    }

    private String cleanClassName(String proxyClassName) {
        // I dont feel proud for this implementation, but it works
        return proxyClassName.replaceAll("_ClientProxy", "");
    }
}
