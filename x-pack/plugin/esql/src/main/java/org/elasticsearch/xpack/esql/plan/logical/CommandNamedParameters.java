/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@code WITH} options on one {@code FROM} or {@code TS} command.
 */
public final class CommandNamedParameters {

    public static final CommandNamedParameters EMPTY = new CommandNamedParameters(Map.of());

    private final Map<String, Option> options;

    public CommandNamedParameters(Map<String, Object> inputs) {
        if (inputs.isEmpty()) {
            this.options = Map.of();
            return;
        }
        Map<String, Option> options = new LinkedHashMap<>(inputs.size());
        for (Map.Entry<String, Object> entry : inputs.entrySet()) {
            options.put(entry.getKey(), new Option(entry.getValue(), true));
        }
        this.options = options;
    }

    /**
     * Keeps each entry's explicit flag. {@code LinkedHashMap} rather than {@code Map} because a second
     * {@code Map} constructor would erase to the same signature as {@link #CommandNamedParameters(Map)}.
     */
    private CommandNamedParameters(LinkedHashMap<String, Option> options) {
        this.options = options;
    }

    public Object get(String key) {
        Option option = options.get(key);
        return option == null ? null : option.value();
    }

    /** Whether this command wrote {@code key}. */
    public boolean explicit(String key) {
        Option option = options.get(key);
        return option != null && option.explicit();
    }

    /**
     * Sets {@code key} to {@code value} when this command did not write it. An explicit value is left unchanged.
     */
    public CommandNamedParameters withInherited(String key, Object value) {
        Option existing = options.get(key);
        if (existing != null && (existing.explicit() || Objects.equals(existing.value(), value))) {
            return this;
        }
        LinkedHashMap<String, Option> updated = new LinkedHashMap<>(options);
        updated.put(key, new Option(value, false));
        return new CommandNamedParameters(updated);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CommandNamedParameters other && options.equals(other.options);
    }

    @Override
    public int hashCode() {
        return options.hashCode();
    }

    @Override
    public String toString() {
        return options.toString();
    }

    /** One {@code WITH} entry. {@code explicit} is true when this command wrote the key. */
    public record Option(Object value, boolean explicit) {}
}
