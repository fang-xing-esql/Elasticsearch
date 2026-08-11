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

public class SubPlanConcurrencyLimiterTests extends ESTestCase {

    public void testLimitsConcurrencyAcrossAllSubmittedLeaves() {
        var harness = new Harness(2);
        harness.submit(0, 1, 2, 3);

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

    public void testFailureRejectsQueuedAndSubsequentLeaves() {
        var harness = new Harness(1);
        harness.submit(0, 1, 2);
        var failure = new IllegalStateException("test failure");

        harness.fail(0, failure);
        harness.submit(3);

        assertThat(harness.started, contains(0));
        assertThat(harness.rejected, contains(1, 2, 3));
        assertThat(harness.rejectionFailures.get(1), equalTo(failure));
        assertThat(harness.rejectionFailures.get(2), equalTo(failure));
        assertThat(harness.rejectionFailures.get(3), equalTo(failure));
    }

    public void testFinishSkipsQueuedAndSubsequentLeaves() {
        var harness = new Harness(1);
        harness.submit(0, 1, 2);

        harness.limiter.finish();
        harness.submit(3);

        assertThat(harness.started, contains(0));
        assertThat(harness.skipped, contains(1, 2, 3));
        harness.complete(0);
    }

    public void testCompletionReleasesPermitOnlyOnce() {
        var harness = new Harness(1);
        harness.submit(0, 1, 2);

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
        var limiter = new SubPlanConcurrencyLimiter<Integer>(
            1,
            Runnable::run,
            (task, listener) -> { throw failure; },
            task -> fail("must not skip task " + task),
            (task, e) -> {
                assertThat(e, equalTo(failure));
                rejected.add(task);
            }
        );

        limiter.submit(0);
        limiter.submit(1);

        assertThat(rejected, contains(0, 1));
        assertThat(limiter.failure(), equalTo(failure));
    }

    public void testSynchronousFailureDoesNotStartAlreadySelectedLeaves() {
        var failure = new IllegalStateException("startup failure");
        List<Integer> started = new ArrayList<>();
        List<Integer> rejected = new ArrayList<>();
        List<Runnable> dispatched = new ArrayList<>();
        var limiter = new SubPlanConcurrencyLimiter<Integer>(3, dispatched::add, (task, listener) -> {
            started.add(task);
            if (task == 0) {
                throw failure;
            }
        }, task -> fail("must not skip task " + task), (task, e) -> {
            assertThat(e, equalTo(failure));
            rejected.add(task);
        });

        limiter.submit(0);
        limiter.submit(1);
        limiter.submit(2);
        dispatched.forEach(Runnable::run);

        assertThat(started, contains(0));
        assertThat(rejected, containsInAnyOrder(0, 1, 2));
    }

    private static final class Harness {
        private final AtomicInteger running = new AtomicInteger();
        private final AtomicInteger maxRunning = new AtomicInteger();
        private final List<Integer> started = new ArrayList<>();
        private final List<Integer> skipped = new ArrayList<>();
        private final List<Integer> rejected = new ArrayList<>();
        private final Map<Integer, ActionListener<Void>> completions = new HashMap<>();
        private final Map<Integer, Exception> rejectionFailures = new HashMap<>();
        private final SubPlanConcurrencyLimiter<Integer> limiter;

        private Harness(int degree) {
            limiter = new SubPlanConcurrencyLimiter<>(degree, Runnable::run, (task, completion) -> {
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
                limiter.submit(task);
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
