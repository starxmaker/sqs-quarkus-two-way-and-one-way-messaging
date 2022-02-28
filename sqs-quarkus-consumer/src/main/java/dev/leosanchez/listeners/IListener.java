package dev.leosanchez.listeners;

import java.util.Optional;

public interface IListener {
    public Optional<String> process(String message);
}
