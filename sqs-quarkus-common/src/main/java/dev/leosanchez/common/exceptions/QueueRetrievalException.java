package dev.leosanchez.common.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class QueueRetrievalException extends Exception {
    public QueueRetrievalException(String message) {
        super(message);
    }
}