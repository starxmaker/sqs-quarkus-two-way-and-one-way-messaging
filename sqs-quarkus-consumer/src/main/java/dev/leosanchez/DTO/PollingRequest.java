package dev.leosanchez.DTO;

import java.util.function.Function;

public class PollingRequest {
    private String queueUrl;
    private Function<String, String> processer;
    private boolean parallelProcessing;
    private int maxNumberOfMessagesPerPolling;
    private int minExecutionTime;

    // constructor based on the builder
    public PollingRequest(PollingRequestBuilder builder) {
        this.queueUrl = builder.queueUrl;
        this.processer = builder.processer;
        this.parallelProcessing = builder.parallelProcessing;
        this.maxNumberOfMessagesPerPolling = builder.maxNumberOfMessagesPerPolling;
        this.minExecutionTime = builder.minExecutionTime;
    }

    //simple getters
    public String getQueueUrl() {
        return queueUrl;
    }
    public Function<String, String> getProcesser() {
        return processer;
    }
    public boolean isParallelProcessing() {
        return parallelProcessing;
    }
    public int getMaxNumberOfMessagesPerPolling() {
        return maxNumberOfMessagesPerPolling;
    }
    public int getMinExecutionTime() {
        return minExecutionTime;
    }

    // our builder
    public static class PollingRequestBuilder {
        private String queueUrl;
        private Function<String, String> processer;
        // some default values
        private boolean parallelProcessing = true;
        private int maxNumberOfMessagesPerPolling = 10;
        private int minExecutionTime = 0;

        private PollingRequestBuilder() {
        }

        public static PollingRequestBuilder builder() {
            return new PollingRequestBuilder();
        }

        public PollingRequestBuilder queueUrl(String queueUrl) {
            this.queueUrl = queueUrl;
            return this;
        }

        public PollingRequestBuilder processer(Function<String, String> processer) {
            this.processer = processer;
            return this;
        }

        public PollingRequestBuilder noParallelProcessing() {
            this.parallelProcessing = false;
            return this;
        }

        public PollingRequestBuilder maxNumberOfMessagesPerPolling(int maxNumberOfMessagesPerPolling) {
            this.maxNumberOfMessagesPerPolling = maxNumberOfMessagesPerPolling;
            return this;
        }

        public PollingRequestBuilder minExecutionTime(int minExecutionTime) {
            this.minExecutionTime = minExecutionTime;
            return this;
        }

        public PollingRequest build() {
            return new PollingRequest(this);
        }
    }
    
}
