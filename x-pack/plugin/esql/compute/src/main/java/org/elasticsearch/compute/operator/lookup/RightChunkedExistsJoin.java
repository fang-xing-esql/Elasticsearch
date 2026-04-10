/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.BitSet;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * Performs an existence join (semi or anti) where many "right hand" pages are checked
 * against a "left hand" {@link Page}. Unlike {@link RightChunkedLeftJoin}, this never
 * fans out left rows — each left row is emitted at most once.
 * <p>
 * The "right hand" pages contain a {@code positions} column (column 0) indicating which
 * left-hand positions have matches. This class accumulates all matched positions across
 * all right-hand pages, then emits:
 * <ul>
 *     <li><b>Semi join</b>: left rows that have at least one match</li>
 *     <li><b>Anti join</b>: left rows that have no match</li>
 * </ul>
 * <p>
 * No right-side columns are appended to the output — this is a pure existence check.
 */
public class RightChunkedExistsJoin implements RightChunkedJoin {
    private final Page leftHand;
    private final boolean anti;
    private final BitSet matched;

    public RightChunkedExistsJoin(Page leftHand, boolean anti) {
        this.leftHand = leftHand;
        this.anti = anti;
        this.matched = new BitSet(leftHand.getPositionCount());
    }

    /**
     * Consume a right-hand page, recording which left positions have matches.
     * Returns {@code null} — output is deferred until {@link #noMoreRightHandPages()}.
     */
    @Override
    public Page join(Page rightHand) {
        IntBlock positionsBlock = rightHand.getBlock(0);
        IntVector positions = positionsBlock.asVector();
        if (positions != null) {
            for (int p = 0; p < positions.getPositionCount(); p++) {
                matched.set(positions.getInt(p));
            }
        } else {
            for (int p = 0; p < positionsBlock.getPositionCount(); p++) {
                int valueCount = positionsBlock.getValueCount(p);
                int firstIdx = positionsBlock.getFirstValueIndex(p);
                for (int v = 0; v < valueCount; v++) {
                    matched.set(positionsBlock.getInt(firstIdx + v));
                }
            }
        }
        return null;
    }

    /**
     * Emit the final result: left rows filtered by existence (semi) or absence (anti) of matches.
     */
    @Override
    public Optional<Page> noMoreRightHandPages() {
        int leftPositions = leftHand.getPositionCount();
        int[] filter = IntStream.range(0, leftPositions).filter(i -> anti != matched.get(i)).toArray();
        if (filter.length == 0) {
            return Optional.empty();
        }
        Block[] blocks = new Block[leftHand.getBlockCount()];
        try {
            for (int b = 0; b < leftHand.getBlockCount(); b++) {
                blocks[b] = leftHand.getBlock(b).filter(true, filter);
            }
            Page result = new Page(blocks);
            blocks = null;
            return Optional.of(result);
        } finally {
            if (blocks != null) {
                Releasables.close(blocks);
            }
        }
    }

    @Override
    public void releaseOnAnyThread() {
        leftHand.allowPassingToDifferentDriver();
        leftHand.releaseBlocks();
    }

    @Override
    public void close() {
        Releasables.close(leftHand::releaseBlocks);
    }
}
