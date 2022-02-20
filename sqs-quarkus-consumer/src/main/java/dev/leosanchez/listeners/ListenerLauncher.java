package dev.leosanchez.listeners;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import dev.leosanchez.DTO.QueueMessage;
import dev.leosanchez.annotations.Listen;
import dev.leosanchez.providers.QueueConsumerProvider.IQueueConsumerProvider;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.configuration.ProfileManager;

@Startup
@ApplicationScoped
public class ListenerLauncher {

    private static final Logger LOG = Logger.getLogger(ListenerLauncher.class);
    
    @Inject
    IQueueConsumerProvider queueProvider;

    // if we are using native-image, we need to declare our listeners here. Otherwise, we could use the reflections library to find them
    List <Class<?>> listenerClasses = List.of(
        CoordinatesListener.class
    );

    ExecutorService executorService = Executors.newFixedThreadPool(getNumberOfListeners(listenerClasses));

    public Integer getNumberOfListeners(List<Class<?>> listenerClasses) {
        Integer quantity = 0;
        for (Class<?> listener : listenerClasses) {
            for (Method method : listener.getDeclaredMethods()) {
                if (method.isAnnotationPresent(Listen.class)) {
                    quantity++;
                }
            }
        }
        return quantity;
    }

    @PostConstruct
    public void launch() {
        if(!ProfileManager.getActiveProfile().equals("test")) {
            startListenerClasses(listenerClasses);
        }
    }

   
    public List<Future<?>> startListenerClasses(List<Class<?>> listenerClasses){
        List<Future<?>> tasks = new ArrayList<>();
        try {
            // we check every defined class
            for (Class<?> listener : listenerClasses) {
                // we check each one of their methods
                for (Method method : listener.getDeclaredMethods()) {
                    // we filter the ones with the annotation
                    if (method.isAnnotationPresent(Listen.class)) {
                        // we obtain the metadata from the annotation
                        Listen listen = method.getAnnotation(Listen.class);
                        // we obtain the queue url from the url property of the annotation
                        String url = ConfigProvider.getConfig().getValue(listen.urlProperty(), String.class);
                        // we obtain if the listener was configured to parallel processing or not
                        Boolean parallelProcessing = listen.parallelProcessing();
                        LOG.info("Starting listener: " + listener.getName() + " with url: " + url);
                        // and we start the listener
                        Future<?> task = startListener(method, url, parallelProcessing, listener.getConstructor().newInstance());
                        tasks.add(task);
                    }
                }
            }
        }
        catch (Exception e) {
            LOG.error("Error starting listeners", e);
        }
        return tasks;
    }

    private Future<?> startListener(Method listenerMethod, String queueUrl, Boolean parallelProcessing, Object instance) {
        Future<?> task = executorService.submit(() -> {
            try {
                // we start listening
                while(!Thread.interrupted()) {
                    LOG.info("polling messages for queue "+queueUrl);
                    // we poll messages from the queue
                    List<QueueMessage> messages = queueProvider.pollMessages(queueUrl);
                    if(messages.isEmpty()){
                        LOG.info("No messages received for queue "+queueUrl);
                        continue;
                    } else {
                        // if we receive a message, we start processing
                        LOG.info("Received " + messages.size() + " messages");
                        Consumer<QueueMessage> consumer = message -> {
                            // we invoke the method
                            String response = processMessage(message, listenerMethod, instance);
                            // if the response was not null we send it to the source queue according to its signature
                            if(Objects.nonNull(response)){
                                LOG.infov("Sending response: {0} {1} {2}", response, message.getSourceQueueUrl(), message.getSignature());
                                queueProvider.sendAnswer(message.getSourceQueueUrl(), response, message.getSignature());
                            }
                        };
                        if(parallelProcessing){
                            messages.parallelStream().forEach(consumer);    
                        } else {
                            messages.stream().forEach(consumer);
                        }
                        
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("Queue "+queueUrl+" was stopped due to an error", e);
            }
        });
        return task;
    }

    private String processMessage(QueueMessage message, Method method, Object instance) {
        LOG.info("Processing message");
        // by default the response is null
        try {
            // we invoke the method received (we have to pass the current instance)
            Object rawResponse = method.invoke(instance, message.getMessage());
            // if the response is not null (in other words, the communication is bidirectional) we transform it to string and return it
            if(Objects.nonNull(rawResponse)){
                return rawResponse.toString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // if no response was returned (in other words, the communication is unidirectional) or if an error appeared we return null
        return null;
    }
}
