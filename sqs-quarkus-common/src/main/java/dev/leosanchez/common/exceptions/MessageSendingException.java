package dev.leosanchez.common.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class MessageSendingException extends Exception {
    public MessageSendingException(String message) {
        super(message);
    }
}