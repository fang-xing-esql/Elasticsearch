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
     * Factor applied to page bytes to estimate GC lagging overhead when a page enters the exchange sink.
     * Configurable via the {@code esql.gc_overhead_factor} cluster setting.
     */
    private final double gcOverheadFactor;

    /**
     * Fraction of accumulated GC overhead released each time a new page is added, modeling GC
     * gradually catching up. Configurable via the {@code esql.gc_decay_factor} cluster setting.
     */
    private final double gcDecayFactor;

    /**
     * Minimum page size (in bytes) required to apply GC lagging overhead. Pages whose
     * {@code ramBytesUsedByBlocks()} is below this threshold skip the overhead entirely.
     * Derived from {@code esql.values_loading_jumbo_size}.
     */
    private final long gcOverheadJumboThreshold;

    /**
     * Lock protecting {@link #gcLaggingOverheadBytes} and {@link #gcOverheadReleased} to prevent
     * races between adding overhead (driver threads) and releasing overhead (cleanup threads).
     */
    private final Object gcOverheadLock = new Object();

    /**
     * Cumulative bytes tracked on the circuit breaker for estimated GC lagging overhead.
     * This tracks untracked UTF-16 String garbage from source parsing that lingers in JVM memory.
     * Guarded by {@link #gcOverheadLock}.
     */
    private long gcLaggingOverheadBytes;

    /**
     * Set to true once {@link #releaseGcLaggingOverhead()} has been called, preventing further
     * overhead from being added. Guarded by {@link #gcOverheadLock}.
     */
    private boolean gcOverheadReleased;

    public ExchangeSinkHandler(
        BlockFactory blockFactory,
        int maxBufferSize,
        LongSupplier nowInMillis,
        double gcOverheadFactor,
        double gcDecayFactor,
        long gcOverheadJumboThreshold
    ) {
        this.blockFactory = blockFactory;
        this.buffer = new ExchangeBuffer(maxBufferSize);
        this.completionFuture = SubscribableListener.newForked(buffer::addCompletionListener);
        this.completionFuture.addListener(ActionListener.running(this::releaseGcLaggingOverhead));
        this.nowInMillis = nowInMillis;
        this.lastUpdatedInMillis = new AtomicLong(nowInMillis.getAsLong());
        this.gcOverheadFactor = gcOverheadFactor;
        this.gcDecayFactor = gcDecayFactor;
        this.gcOverheadJumboThreshold = gcOverheadJumboThreshold;
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
            // HeapAttackIT.testFetchMvLongs builds a 80MB giant page with 100 documents in it. How does it happen?
            // Check the average document size, targeting to giant text fields similar to ValuesFromSingerReader and
            // ValuesFromManyReader only when adding overhead for gc
            long averageDocumentSize = page.getPositionCount() > 0 ? page.ramBytesUsedByBlocks() / page.getPositionCount() : 0;
            if (averageDocumentSize >= gcOverheadJumboThreshold) {
                boolean success = false;
                try {
                    addGcLaggingOverhead(page);
                    success = true;
                } finally {
                    if (success == false) {
                        // release the memory tracked by circuit breaker to prevent leaks,
                        // otherwise circuit break report non-zero size after circuit breaker trips
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
        releaseGcLaggingOverhead();
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
     * Adds GC lagging overhead to the circuit breaker when a page enters the exchange sink(and the
     * other pipeline breakers potentially). First decays existing overhead (modeling GC catching up),
     * then adds new overhead proportional to the page's block data size. This models the lingering
     * UTF-16 String objects from source parsing that haven't been collected by GC yet.
     * <p>
     * By tracking this overhead only at the exchange sink level (not per-driver in ValuesSourceReader),
     * single-pipeline queries (where text blocks are consumed immediately by eval/aggregation) incur
     * no GC overhead penalty. Only multi-pipeline (subquery) queries that route pages through exchange
     * buffers accumulate this overhead, causing the circuit breaker to trip before OOM.
     * <p>
     * Synchronized on {@link #gcOverheadLock} to prevent races with {@link #releaseGcLaggingOverhead()}.
     */
    private void addGcLaggingOverhead(Page page) {
        long pageBytes = page.ramBytesUsedByBlocks();
        if (pageBytes <= 0) {
            return;
        }
        synchronized (gcOverheadLock) {
            if (gcOverheadReleased) {
                return;
            }
            // Decay existing overhead, modeling GC gradually collecting humongous objects
            if (gcLaggingOverheadBytes > 0) {
                long release = (long) (gcLaggingOverheadBytes * gcDecayFactor);
                if (release > 0) {
                    blockFactory.breaker().addWithoutBreaking(-release);
                    gcLaggingOverheadBytes -= release;
                }
            }
            // Add new overhead for this page
            long overhead = (long) (pageBytes * gcOverheadFactor);
            if (overhead > 0) {
                blockFactory.breaker().addEstimateBytesAndMaybeBreak(overhead, "exchange sink add page gc overhead");
                gcLaggingOverheadBytes += overhead;
            }
        }
    }

    /**
     * Releases all accumulated GC lagging overhead from the circuit breaker.
     * Called when the sink handler completes or fails. Sets {@link #gcOverheadReleased} to prevent
     * any further overhead from being added by concurrent driver threads.
     */
    private void releaseGcLaggingOverhead() {
        synchronized (gcOverheadLock) {
            gcOverheadReleased = true;
            if (gcLaggingOverheadBytes > 0) {
                blockFactory.breaker().addWithoutBreaking(-gcLaggingOverheadBytes);
                logger.debug("released {} bytes of GC lagging overhead from exchange sink", gcLaggingOverheadBytes);
                gcLaggingOverheadBytes = 0;
            }
        }
    }
}
