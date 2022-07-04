package dev.leosanchez.common.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class MessageRemovalException extends Exception {
    public MessageRemovalException(String message) {
        super(message);
    }
}
