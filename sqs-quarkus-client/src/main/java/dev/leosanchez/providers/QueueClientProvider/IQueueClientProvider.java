package dev.leosanchez.providers.QueueClientProvider;

public interface IQueueClientProvider {

    public String sendMessage(String targetQueueUrl, String message);

    public String receiveResponse( String signature, Integer secondsToTimeout);
    
}