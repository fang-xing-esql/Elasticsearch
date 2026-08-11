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
import java.util.function.Predicate;

/**
 * Non-blocking query-wide concurrency limiter for leaf subplan producers. Task callbacks are always invoked outside the scheduler lock.
 */
final class SubPlanLeafScheduler<T> {
    private final int maxRunning;
    private final Executor executor;
    private final BiConsumer<T, ActionListener<Void>> starter;
    private final Consumer<T> skipper;
    private final BiConsumer<T, Exception> rejecter;
    private final Deque<T> pending = new ArrayDeque<>();
    private int running;
    private boolean started;
    private Exception failure;

    SubPlanLeafScheduler(
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
        List<T> ready = List.of();
        synchronized (this) {
            rejection = failure;
            if (rejection == null) {
                pending.addLast(task);
                ready = takeReadyLocked();
            }
        }
        if (rejection == null) {
            dispatch(ready);
        } else {
            rejecter.accept(task, rejection);
        }
    }

    void start() {
        List<T> ready;
        synchronized (this) {
            started = true;
            ready = takeReadyLocked();
        }
        dispatch(ready);
    }

    void finishPending(Predicate<T> predicate) {
        List<T> skipped;
        synchronized (this) {
            skipped = drainLocked(predicate);
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
            rejected = drainLocked(task -> true);
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
                rejected = drainLocked(task -> true);
            }
            ready = takeReadyLocked();
        }
        if (taskFailure != null) {
            rejected.forEach(task -> rejecter.accept(task, taskFailure));
        }
        dispatch(ready);
    }

    private List<T> takeReadyLocked() {
        if (started == false || failure != null || running >= maxRunning || pending.isEmpty()) {
            return List.of();
        }
        List<T> ready = new ArrayList<>(Math.min(maxRunning - running, pending.size()));
        while (running < maxRunning && pending.isEmpty() == false) {
            running++;
            ready.add(pending.removeFirst());
        }
        return ready;
    }

    private List<T> drainLocked(Predicate<T> predicate) {
        List<T> drained = new ArrayList<>();
        pending.removeIf(task -> {
            if (predicate.test(task)) {
                drained.add(task);
                return true;
            }
            return false;
        });
        return drained;
    }

    private void dispatch(List<T> ready) {
        for (T task : ready) {
            if (rejectIfFailed(task)) {
                continue;
            }
            try {
                executor.execute(() -> {
                    if (rejectIfFailed(task)) {
                        return;
                    }
                    ActionListener<Void> completion = ActionListener.notifyOnce(
                        ActionListener.wrap(ignored -> taskFinished(null), SubPlanLeafScheduler.this::taskFinished)
                    );
                    try {
                        starter.accept(task, completion);
                    } catch (Exception e) {
                        rejecter.accept(task, e);
                        completion.onFailure(e);
                    }
                });
            } catch (Exception e) {
                taskFinished(e);
                rejecter.accept(task, e);
            }
        }
    }

    private boolean rejectIfFailed(T task) {
        Exception rejection = failure();
        if (rejection == null) {
            return false;
        }
        taskFinished(rejection);
        rejecter.accept(task, rejection);
        return true;
    }
}
