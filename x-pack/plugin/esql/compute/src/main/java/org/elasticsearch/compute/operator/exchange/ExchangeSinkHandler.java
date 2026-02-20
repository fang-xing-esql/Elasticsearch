/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * An {@link ExchangeSinkHandler} receives pages and status from its {@link ExchangeSink}s, which are created using
 * {@link #createExchangeSink(Runnable)}} method. Pages and status can then be retrieved asynchronously by {@link ExchangeSourceHandler}s
 * using the {@link #fetchPageAsync(boolean, ActionListener)} method.
 *
 * @see #createExchangeSink(Runnable)
 * @see #fetchPageAsync(boolean, ActionListener)
 * @see ExchangeSourceHandler
 */
public final class ExchangeSinkHandler {

    private static final Logger logger = LogManager.getLogger(ExchangeSinkHandler.class);

    private final ExchangeBuffer buffer;
    private final Queue<ActionListener<ExchangeResponse>> listeners = new ConcurrentLinkedQueue<>();
    private final AtomicInteger outstandingSinks = new AtomicInteger();
    // listeners are notified by only one thread.
    private final Semaphore promised = new Semaphore(1);

    private final SubscribableListener<Void> completionFuture;
    private final LongSupplier nowInMillis;
    private final AtomicLong lastUpdatedInMillis;
    private final BlockFactory blockFactory;

    /**
     * Penalty factor applied to page bytes when a page with large documents enters the exchange sink.
     * Configurable via the {@code esql.page_penalty_factor} cluster setting.
     */
    private final double pagePenaltyFactor;

    /**
     * Minimum average document size (in bytes) required to apply the page penalty. Pages whose
     * average document size is below this threshold skip the penalty entirely.
     * Derived from {@code esql.values_loading_jumbo_size}.
     */
    private final long pagePenaltyJumboThreshold;

    /**
     * Lock protecting {@link #pagePenaltyBytes} and {@link #pagePenaltyReleased} to prevent
     * races between adding penalty (driver threads) and releasing penalty (cleanup threads).
     */
    private final Object pagePenaltyLock = new Object();

    /**
     * Cumulative bytes tracked on the circuit breaker for page penalty overhead.
     * Guarded by {@link #pagePenaltyLock}.
     */
    private long pagePenaltyBytes;

    /**
     * Set to true once {@link #releasePagePenalty()} has been called, preventing further
     * penalty from being added. Guarded by {@link #pagePenaltyLock}.
     */
    private boolean pagePenaltyReleased;

    public ExchangeSinkHandler(
        BlockFactory blockFactory,
        int maxBufferSize,
        LongSupplier nowInMillis,
        double pagePenaltyFactor,
        long pagePenaltyJumboThreshold
    ) {
        this.blockFactory = blockFactory;
        this.buffer = new ExchangeBuffer(maxBufferSize);
        this.completionFuture = SubscribableListener.newForked(buffer::addCompletionListener);
        this.completionFuture.addListener(ActionListener.running(this::releasePagePenalty));
        this.nowInMillis = nowInMillis;
        this.lastUpdatedInMillis = new AtomicLong(nowInMillis.getAsLong());
        this.pagePenaltyFactor = pagePenaltyFactor;
        this.pagePenaltyJumboThreshold = pagePenaltyJumboThreshold;
    }

    private class ExchangeSinkImpl implements ExchangeSink {
        boolean finished;
        private final Runnable onPageFetched;
        private final SubscribableListener<Void> onFinished = new SubscribableListener<>();

        ExchangeSinkImpl(Runnable onPageFetched) {
            this.onPageFetched = onPageFetched;
            onChanged();
            buffer.addCompletionListener(onFinished);
            outstandingSinks.incrementAndGet();
        }

        @Override
        public void addPage(Page page) {
            onPageFetched.run();
            if (page.averageRowSize() >= pagePenaltyJumboThreshold) {
                boolean success = false;
                try {
                    addPagePenalty(page);
                    success = true;
                } finally {
                    if (success == false) {
                        page.releaseBlocks();
                    }
                }
            }
            buffer.addPage(page);
            notifyListeners();
        }

        @Override
        public void finish() {
            if (finished == false) {
                finished = true;
                onFinished.onResponse(null);
                onChanged();
                if (outstandingSinks.decrementAndGet() == 0) {
                    buffer.finish(false);
                    notifyListeners();
                }
            }
        }

        @Override
        public boolean isFinished() {
            return onFinished.isDone();
        }

        @Override
        public void addCompletionListener(ActionListener<Void> listener) {
            onFinished.addListener(listener);
        }

        @Override
        public IsBlockedResult waitForWriting() {
            return buffer.waitForWriting();
        }
    }

    /**
     * Fetches pages and the sink status asynchronously.
     *
     * @param sourceFinished if true, then this handler can finish as sources have enough pages.
     * @param listener       the listener that will be notified when pages are ready or this handler is finished
     * @see RemoteSink
     * @see ExchangeSourceHandler#addRemoteSink(RemoteSink, boolean, Runnable, int, ActionListener)
     */
    public void fetchPageAsync(boolean sourceFinished, ActionListener<ExchangeResponse> listener) {
        if (sourceFinished) {
            buffer.finish(true);
        }
        listeners.add(listener);
        onChanged();
        notifyListeners();
    }

    /**
     * Add a listener, which will be notified when this exchange sink handler is completed. An exchange sink
     * handler is consider completed when all associated sinks are completed and the output pages are fetched.
     */
    public void addCompletionListener(ActionListener<Void> listener) {
        completionFuture.addListener(listener);
    }

    /**
     * Returns true if an exchange is finished
     */
    public boolean isFinished() {
        return completionFuture.isDone();
    }

    /**
     * Fails this sink exchange handler
     */
    void onFailure(Exception failure) {
        releasePagePenalty();
        completionFuture.onFailure(failure);
        buffer.finish(true);
        notifyListeners();
    }

    private void notifyListeners() {
        while (listeners.isEmpty() == false && (buffer.size() > 0 || buffer.noMoreInputs())) {
            if (promised.tryAcquire() == false) {
                break;
            }
            final ActionListener<ExchangeResponse> listener;
            final ExchangeResponse response;
            try {
                // Use `poll` and recheck because `listeners.isEmpty()` might return true, while a listener is being added
                listener = listeners.poll();
                if (listener == null) {
                    continue;
                }
                response = new ExchangeResponse(blockFactory, buffer.pollPage(), buffer.isFinished());
            } finally {
                promised.release();
            }
            onChanged();
            ActionListener.respondAndRelease(listener, response);
        }
    }

    /**
     * Create a new exchange sink for exchanging data
     *
     * @param onPageFetched a {@link Runnable} that will be called when a page is fetched.
     * @see ExchangeSinkOperator
     */
    public ExchangeSink createExchangeSink(Runnable onPageFetched) {
        return new ExchangeSinkImpl(onPageFetched);
    }

    /**
     * Whether this sink handler has sinks attached or available pages
     */
    boolean hasData() {
        return outstandingSinks.get() > 0 || buffer.size() > 0;
    }

    /**
     * Whether this sink handler has listeners waiting for data
     */
    boolean hasListeners() {
        return listeners.isEmpty() == false;
    }

    private void onChanged() {
        lastUpdatedInMillis.accumulateAndGet(nowInMillis.getAsLong(), Math::max);
    }

    /**
     * The time in millis when this sink handler was updated. This timestamp is used to prune idle sinks.
     *
     * @see ExchangeService#INACTIVE_SINKS_INTERVAL_SETTING
     */
    long lastUpdatedTimeInMillis() {
        return lastUpdatedInMillis.get();
    }

    /**
     * Returns the number of pages available in the buffer.
     * This method should be used for testing only.
     */
    public int bufferSize() {
        return buffer.size();
    }

    /**
     * Adds a penalty to the circuit breaker proportional to the page size. This accounts for
     * untracked JVM heap pressure (e.g. loading large objects from _source)
     * that accumulates when pages flow through pipeline breakers like exchange sinks.
     * <p>
     * Synchronized on {@link #pagePenaltyLock} to prevent races with {@link #releasePagePenalty()}.
     */
    private void addPagePenalty(Page page) {
        long pageBytes = page.ramBytesUsedByBlocks();
        if (pageBytes <= 0) {
            return;
        }
        synchronized (pagePenaltyLock) {
            if (pagePenaltyReleased) {
                return;
            }
            long penalty = (long) (pageBytes * pagePenaltyFactor);
            if (penalty > 0) {
                blockFactory.breaker().addEstimateBytesAndMaybeBreak(penalty, "exchange sink page penalty");
                pagePenaltyBytes += penalty;
            }
        }
    }

    /**
     * Releases all accumulated page penalty from the circuit breaker.
     * Called when the sink handler completes or fails. Sets {@link #pagePenaltyReleased} to prevent
     * any further penalty from being added by concurrent driver threads.
     */
    private void releasePagePenalty() {
        synchronized (pagePenaltyLock) {
            pagePenaltyReleased = true;
            if (pagePenaltyBytes > 0) {
                blockFactory.breaker().addWithoutBreaking(-pagePenaltyBytes);
                logger.debug("released {} bytes of page penalty from exchange sink", pagePenaltyBytes);
                pagePenaltyBytes = 0;
            }
        }
    }
}
