/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class SubPlanLeafSchedulerTests extends ESTestCase {

    public void testQueryWideConcurrencyLimitAndProgress() {
        var harness = new Harness(2);
        harness.submit(0, 1, 2, 3);

        harness.scheduler.start();
        assertThat(harness.started, contains(0, 1));
        assertThat(harness.maxRunning.get(), equalTo(2));

        harness.complete(0);
        assertThat(harness.started, contains(0, 1, 2));
        harness.complete(1);
        assertThat(harness.started, contains(0, 1, 2, 3));
        harness.complete(2);
        harness.complete(3);

        assertThat(harness.running.get(), equalTo(0));
        assertThat(harness.rejected, empty());
    }

    public void testDegreeOneDoesNotDeadlockQueuedLeaves() {
        var harness = new Harness(1);
        harness.submit(0, 1, 2);
        harness.scheduler.start();

        assertThat(harness.started, contains(0));
        harness.complete(0);
        assertThat(harness.started, contains(0, 1));
        harness.complete(1);
        assertThat(harness.started, contains(0, 1, 2));
        harness.complete(2);
        assertThat(harness.running.get(), equalTo(0));
    }

    public void testFailureRejectsAllQueuedLeaves() {
        var harness = new Harness(1);
        harness.submit(0, 1, 2);
        harness.scheduler.start();

        var failure = new IllegalStateException("test failure");
        harness.fail(0, failure);

        assertThat(harness.started, contains(0));
        assertThat(harness.rejected, contains(1, 2));
        assertThat(harness.rejectionFailures.get(1), equalTo(failure));
        assertThat(harness.rejectionFailures.get(2), equalTo(failure));
        assertThat(harness.scheduler.failure(), equalTo(failure));
    }

    public void testFinishPendingSkipsOnlyMatchingQueuedLeaves() {
        var harness = new Harness(1);
        harness.submit(0, 1, 2, 3);
        harness.scheduler.start();

        harness.scheduler.finishPending(i -> i == 1 || i == 3);
        assertThat(harness.skipped, contains(1, 3));
        harness.complete(0);
        assertThat(harness.started, contains(0, 2));
        harness.complete(2);
    }

    public void testCompletionIsReleaseOnce() {
        var harness = new Harness(1);
        harness.submit(0, 1, 2);
        harness.scheduler.start();

        ActionListener<Void> first = harness.completions.get(0);
        harness.running.decrementAndGet();
        first.onResponse(null);
        first.onResponse(null);

        assertThat(harness.started, contains(0, 1));
        harness.complete(1);
        assertThat(harness.started, contains(0, 1, 2));
        harness.complete(2);
    }

    public void testSynchronousStartupFailureRejectsCurrentAndQueuedLeaves() {
        var failure = new IllegalStateException("startup failure");
        List<Integer> rejected = new ArrayList<>();
        var scheduler = new SubPlanLeafScheduler<Integer>(
            1,
            Runnable::run,
            (task, listener) -> { throw failure; },
            task -> fail("must not skip task " + task),
            (task, e) -> {
                assertThat(e, equalTo(failure));
                rejected.add(task);
            }
        );
        scheduler.submit(0);
        scheduler.submit(1);
        scheduler.start();

        assertThat(rejected, contains(0, 1));
        assertThat(scheduler.failure(), equalTo(failure));
    }

    public void testSynchronousFailureDoesNotStartAlreadySelectedLeaves() {
        var failure = new IllegalStateException("startup failure");
        List<Integer> started = new ArrayList<>();
        List<Integer> rejected = new ArrayList<>();
        var scheduler = new SubPlanLeafScheduler<Integer>(3, Runnable::run, (task, listener) -> {
            started.add(task);
            if (task == 0) {
                throw failure;
            }
        }, task -> fail("must not skip task " + task), (task, e) -> {
            assertThat(e, equalTo(failure));
            rejected.add(task);
        });
        scheduler.submit(0);
        scheduler.submit(1);
        scheduler.submit(2);
        scheduler.start();

        assertThat(started, contains(0));
        assertThat(rejected, containsInAnyOrder(0, 1, 2));
    }

    public void testSubmitAfterStartDispatchesWhenCapacityIsAvailable() {
        var harness = new Harness(2);
        harness.scheduler.start();
        harness.submit(0, 1);

        assertThat(harness.started, contains(0, 1));
        harness.complete(0);
        harness.complete(1);
    }

    public void testExecutorRejectionReleasesPermitAndRejectsAllLeaves() {
        var failure = new IllegalStateException("executor rejected");
        List<Integer> rejected = new ArrayList<>();
        var scheduler = new SubPlanLeafScheduler<Integer>(
            1,
            command -> { throw failure; },
            (task, listener) -> fail("must not start task " + task),
            task -> fail("must not skip task " + task),
            (task, e) -> {
                assertThat(e, equalTo(failure));
                rejected.add(task);
            }
        );
        scheduler.submit(0);
        scheduler.submit(1);
        scheduler.start();

        assertThat(rejected, containsInAnyOrder(0, 1));
        assertThat(scheduler.failure(), equalTo(failure));
    }

    private static final class Harness {
        private final AtomicInteger running = new AtomicInteger();
        private final AtomicInteger maxRunning = new AtomicInteger();
        private final List<Integer> started = new ArrayList<>();
        private final List<Integer> skipped = new ArrayList<>();
        private final List<Integer> rejected = new ArrayList<>();
        private final Map<Integer, ActionListener<Void>> completions = new HashMap<>();
        private final Map<Integer, Exception> rejectionFailures = new HashMap<>();
        private final SubPlanLeafScheduler<Integer> scheduler;

        private Harness(int degree) {
            scheduler = new SubPlanLeafScheduler<>(degree, Runnable::run, (task, completion) -> {
                started.add(task);
                int current = running.incrementAndGet();
                maxRunning.accumulateAndGet(current, Math::max);
                completions.put(task, completion);
            }, skipped::add, (task, failure) -> {
                rejected.add(task);
                rejectionFailures.put(task, failure);
            });
        }

        private void submit(Integer... tasks) {
            for (Integer task : tasks) {
                scheduler.submit(task);
            }
        }

        private void complete(int task) {
            running.decrementAndGet();
            completions.get(task).onResponse(null);
        }

        private void fail(int task, Exception failure) {
            running.decrementAndGet();
            completions.get(task).onFailure(failure);
        }
    }
}
