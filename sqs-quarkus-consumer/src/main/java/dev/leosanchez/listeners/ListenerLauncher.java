package dev.leosanchez.listeners;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import dev.leosanchez.DTO.QueueMessage;
import dev.leosanchez.providers.QueueConsumerProvider.IQueueConsumerProvider;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.configuration.ProfileManager;

@ApplicationScoped
@Startup
public class ListenerLauncher {

    // a simple logger
    private static Logger LOG = Logger.getLogger(ListenerLauncher.class);

    // our listeners filtered by the qualifier
    @ListenerQualifier
    Instance<IListener> listeners;

    // the provider we implemented
    @Inject
    IQueueConsumerProvider queueConsumerProvider;

    ExecutorService executorService;

    @PostConstruct
    public void init() {
        // we just want to launch the listeners if the profile is not test
        if (!ProfileManager.getActiveProfile().equals("test")) {
            LOG.info("Launching listeners");
            List<IListener> listenerList = listeners.stream().collect(java.util.stream.Collectors.toList());
            startListeners(listenerList);
        }
    }

    public void startListeners(List<IListener> listenersList) {
        // we create a thread pool with the number of listeners
        executorService = Executors.newFixedThreadPool(listenersList.size());
        for (IListener listener : listenersList) {
            // we extract the Original Class name from the proxy
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
            // We get our annotation
            ListenerQualifier annotation = listenerClass.getAnnotation(ListenerQualifier.class);

            // We extract the metadata
            String urlProperty = annotation.urlProperty();
            if (urlProperty.equals("")) {
                // if no url property, skip
                LOG.error("No queue url property for listener " + listenerClassName + " was provided. Skipping...");
                continue;
            }
            boolean parallelProcessing = annotation.parallelProcessing();
            int minProcessingMilliseconds = annotation.minProcessingMilliseconds();
            int maxNumberOfMessagesPerProcessing = annotation.maxNumberOfMessagesPerProcessing();

            // obtaining url from properties
            String queryUrl = ConfigProvider.getConfig().getValue(urlProperty, String.class);
            // we launch the specific listener as a task to submit on our thread pool
            executorService.submit(() -> {
                startListener(listener, queryUrl, parallelProcessing, maxNumberOfMessagesPerProcessing,
                        minProcessingMilliseconds);
            });
        }
    }

    public void startListener(IListener listener, String url, boolean parallelProcessing,
            int maxNumberOfMessagesPerProcessing, int minProcessingMilliseconds) {
        try {
            // we start listening
            while (!Thread.interrupted()) {
                LOG.info("polling messages for queue " + url);
                // we poll messages from the queue
                List<QueueMessage> messages = queueConsumerProvider.pollMessages(url, maxNumberOfMessagesPerProcessing);
                if (messages.isEmpty()) {
                    LOG.info("No messages received for queue" + url);
                    continue;
                } else {
                    // if we receive a message, we start processing
                    LOG.info("Received " + messages.size() + " messages");
                    // we configure a consumer for the messages we receive
                    Consumer<QueueMessage> consumer = message -> {
                        onMessage(message, listener, minProcessingMilliseconds);
                    };
                    // if we configured parallelism, we use it
                    if (parallelProcessing) {
                        messages.parallelStream().forEach(consumer);
                    } else {
                        // if not, the messages will be processed sequentially
                        messages.stream().forEach(consumer);
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Queue " + url + " was stopped due to an error", e);
        }
    }

    private void onMessage(QueueMessage message, IListener listener, int minProcessingMilliseconds) {
        Long startExecution = System.currentTimeMillis();
        // we invoke the method
        Optional<String> response = listener.process(message.getMessage());
        // if the response was not null we send it to the source queue according to its
        // signature
        if (response.isPresent()) {
            LOG.infov("Sending response: {0} {1} {2}", response, message.getSourceQueueUrl(),
                    message.getSignature());
            queueConsumerProvider.sendAnswer(message.getSourceQueueUrl(), response.get(), message.getSignature());
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
