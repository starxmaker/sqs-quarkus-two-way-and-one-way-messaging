package dev.leosanchez.common.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class MessagePollingException extends Exception {
    public MessagePollingException(String message) {
        super(message);
    }
}
