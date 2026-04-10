/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

import java.util.Optional;

/**
 * Common interface for joining "right hand" pages against a "left hand" page.
 * <p>
 * Implementations receive right-hand pages one at a time via {@link #join(Page)} and
 * signal completion via {@link #noMoreRightHandPages()}.
 */
public interface RightChunkedJoin extends Releasable {

    /**
     * Join one right-hand page against the left-hand page.
     *
     * @return a joined output page, or {@code null} if this implementation defers output
     *         (e.g., existence joins that accumulate matches and emit once at the end).
     */
    Page join(Page rightHand);

    /**
     * Called after all right-hand pages have been joined. Returns any remaining output,
     * such as unmatched left rows (LEFT JOIN) or the final filtered result (EXISTS JOIN).
     */
    Optional<Page> noMoreRightHandPages();

    /**
     * Release resources on any thread, rather than just the thread that built this join.
     */
    void releaseOnAnyThread();
}
