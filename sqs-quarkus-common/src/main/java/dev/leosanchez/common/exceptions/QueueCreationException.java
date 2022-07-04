package dev.leosanchez.common.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class QueueCreationException extends Exception {
    public QueueCreationException(String message) {
        super(message);
    }
}
