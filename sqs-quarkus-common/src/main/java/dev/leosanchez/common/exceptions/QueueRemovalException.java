package dev.leosanchez.common.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class QueueRemovalException extends Exception {
    public QueueRemovalException(String message) {
        super(message);
    }
}
