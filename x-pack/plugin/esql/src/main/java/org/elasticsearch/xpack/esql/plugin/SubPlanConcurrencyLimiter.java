/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A non-blocking, query-wide concurrency limiter for leaf fork and subquery plans. Callbacks are invoked outside the limiter lock.
 */
final class SubPlanConcurrencyLimiter<T> {
    private final int maxRunning;
    private final Executor executor;
    private final BiConsumer<T, ActionListener<Void>> starter;
    private final Consumer<T> skipper;
    private final BiConsumer<T, Exception> rejecter;
    private final Deque<T> pending = new ArrayDeque<>();
    private int running;
    private boolean finished;
    private Exception failure;

    SubPlanConcurrencyLimiter(
        int maxRunning,
        Executor executor,
        BiConsumer<T, ActionListener<Void>> starter,
        Consumer<T> skipper,
        BiConsumer<T, Exception> rejecter
    ) {
        if (maxRunning < 1) {
            throw new IllegalArgumentException("maxRunning must be positive");
        }
        this.maxRunning = maxRunning;
        this.executor = executor;
        this.starter = starter;
        this.skipper = skipper;
        this.rejecter = rejecter;
    }

    void submit(T task) {
        Exception rejection;
        boolean skip;
        List<T> ready = List.of();
        synchronized (this) {
            rejection = failure;
            skip = rejection == null && finished;
            if (rejection == null && skip == false) {
                pending.addLast(task);
                ready = takeReadyLocked();
            }
        }
        if (rejection != null) {
            rejecter.accept(task, rejection);
        } else if (skip) {
            skipper.accept(task);
        } else {
            dispatch(ready);
        }
    }

    void finish() {
        List<T> skipped;
        synchronized (this) {
            if (finished || failure != null) {
                return;
            }
            finished = true;
            skipped = drainLocked();
        }
        skipped.forEach(skipper);
    }

    void fail(Exception e) {
        List<T> rejected;
        synchronized (this) {
            if (failure != null) {
                return;
            }
            failure = e;
            rejected = drainLocked();
        }
        rejected.forEach(task -> rejecter.accept(task, e));
    }

    synchronized Exception failure() {
        return failure;
    }

    private void taskFinished(Exception taskFailure) {
        List<T> rejected = List.of();
        List<T> ready;
        synchronized (this) {
            running--;
            assert running >= 0;
            if (taskFailure != null && failure == null) {
                failure = taskFailure;
                rejected = drainLocked();
            }
            ready = takeReadyLocked();
        }
        if (taskFailure != null) {
            rejected.forEach(task -> rejecter.accept(task, taskFailure));
        }
        dispatch(ready);
    }

    private List<T> takeReadyLocked() {
        if (finished || failure != null || running >= maxRunning || pending.isEmpty()) {
            return List.of();
        }
        List<T> ready = new ArrayList<>(Math.min(maxRunning - running, pending.size()));
        while (running < maxRunning && pending.isEmpty() == false) {
            running++;
            ready.add(pending.removeFirst());
        }
        return ready;
    }

    private List<T> drainLocked() {
        List<T> drained = new ArrayList<>(pending);
        pending.clear();
        return drained;
    }

    private void dispatch(List<T> ready) {
        for (T task : ready) {
            if (terminateIfNeeded(task)) {
                continue;
            }
            try {
                executor.execute(() -> {
                    if (terminateIfNeeded(task)) {
                        return;
                    }
                    ActionListener<Void> completion = ActionListener.notifyOnce(
                        ActionListener.wrap(ignored -> taskFinished(null), SubPlanConcurrencyLimiter.this::taskFinished)
                    );
                    try {
                        starter.accept(task, completion);
                    } catch (Exception e) {
                        completion.onFailure(e);
                        rejecter.accept(task, e);
                    }
                });
            } catch (Exception e) {
                taskFinished(e);
                rejecter.accept(task, e);
            }
        }
    }

    private boolean terminateIfNeeded(T task) {
        Exception rejection;
        boolean skip;
        synchronized (this) {
            rejection = failure;
            skip = rejection == null && finished;
            if (rejection == null && skip == false) {
                return false;
            }
            running--;
            assert running >= 0;
        }
        if (rejection != null) {
            rejecter.accept(task, rejection);
        } else {
            skipper.accept(task);
        }
        return true;
    }
}
