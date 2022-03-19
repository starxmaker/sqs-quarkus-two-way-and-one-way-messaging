package dev.leosanchez.DTO;

import java.util.Map;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class QueueMessage {
    private String message;
    private Map<String, String> attributes;
    private String receiptHandle;
    public QueueMessage(String message, String receiptHandle, Map<String, String> attributes) {
        this.message = message;
        this.attributes = attributes;
        this.receiptHandle = receiptHandle;
    }
    public String getMessage() {
        return message;
    }
    public Map<String, String> getAttributes() {
        return attributes;
    }
    public String getReceiptHandle() {
        return receiptHandle;
    }
}
