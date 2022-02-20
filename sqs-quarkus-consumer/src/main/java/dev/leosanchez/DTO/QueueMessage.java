package dev.leosanchez.DTO;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class QueueMessage {
    private String message;
    private String sourceQueueUrl;
    private String signature;

    public QueueMessage(String message, String sourceQueueUrl, String signature) {
        this.message = message;
        this.sourceQueueUrl = sourceQueueUrl;
        this.signature = signature;
    }

    public String getMessage() {
        return message;
    }
    public String getSourceQueueUrl() {
        return sourceQueueUrl;
    }
    public String getSignature() {
        return signature;
    }
}
