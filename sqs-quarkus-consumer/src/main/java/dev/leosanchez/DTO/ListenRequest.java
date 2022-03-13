package dev.leosanchez.DTO;

import dev.leosanchez.listeners.IListener;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class ListenRequest {
    IListener listener;
    String queueUrl;
    boolean parallelProcessing;
    Integer maxMessagesPerPolling;
    Integer minExecutionMilliseconds;

    public ListenRequest(IListener listener, String queueUrl, boolean parallelProcessing, Integer maxMessagesPerPolling, Integer minExecutionMilliseconds) {
        this.listener =  listener;
        this.queueUrl = queueUrl;
        this.parallelProcessing = parallelProcessing;
        this.maxMessagesPerPolling = maxMessagesPerPolling;
        this.minExecutionMilliseconds = minExecutionMilliseconds;
    }

    public IListener getListener() {
        return listener;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public boolean isParallelProcessing() {
        return parallelProcessing;
    }

    public Integer getMaxMessagesPerPolling(){
        return maxMessagesPerPolling;
    }

    public Integer getMinExecutionMilliseconds(){
        return minExecutionMilliseconds;
    }
}
