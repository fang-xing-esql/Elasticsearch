/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

/**
 * Loads values by iterating in original document sequence order instead of
 * the sorted {@code (shard, segment, doc)} order used by {@link ValuesFromManyReader}.
 * This avoids the backwards reorder and supports partial pages bounded by
 * {@link ValuesSourceReaderOperator#jumboBytes}.
 * <p>
 *     Pages may span multiple shards and segments. Whenever the shard, segment,
 *     or doc order changes, {@link ValuesSourceReaderOperator#positionFieldWork}
 *     invalidates non-reusable readers and {@link Run#fieldsMoved} refreshes the
 *     stored-field loader for the new context.
 * </p>
 * <p>
 *     For single-segment non-decreasing pages, {@link DocSequenceSingleSegmentRun}
 *     reads column-at-a-time fields in batches inside the main loop so that actual
 *     bytes are included in the jumbo check, and supports sequential stored field readers.
 * </p>
 */
class ValuesFromDocSequence extends ValuesReader {
    private static final Logger log = LogManager.getLogger(ValuesFromDocSequence.class);

    /**
     * When {@link ValuesSourceReaderOperator#maxColumnAtATimeBytesPerDoc} has not been
     * established yet (first page, no boundaries), flush column-at-a-time after this many
     * positions to estimate the average size of column-at-a-time and prevent unbounded page growth.
     */
    static final int INITIAL_COLUMN_AT_A_TIME_BATCH_SIZE = 5;

    ValuesFromDocSequence(ValuesSourceReaderOperator operator, DocVector docs) {
        super(operator, docs);
        log.debug("initializing {} positions", docs.getPositionCount());
    }

    @Override
    protected void load(Block[] target, int offset) throws IOException {
        if (docs.singleSegmentNonDecreasing()) {
            try (DocSequenceSingleSegmentRun run = new DocSequenceSingleSegmentRun(target)) {
                run.load(offset);
            }
        } else {
            try (DocSequenceRun run = new DocSequenceRun(target)) {
                run.load(offset);
            }
        }
    }

    /**
     * Optimized loading for single-segment non-decreasing pages. Since doc IDs
     * are non-decreasing within a single segment, column-at-a-time readers can advance forward continuously,
     * allowing batch reads inside the main loop. Each batch's actual bytes are
     * included in the jumbo check for page-size control, and
     * {@link ValuesSourceReaderOperator#maxColumnAtATimeBytesPerDoc} is bootstrapped
     * from the first probe batch.
     * <p>
     *     Also supports sequential stored field readers (like {@link ValuesFromSingleReader})
     *     and propagates {@link Block.MvOrdering} from column-at-a-time blocks.
     * </p>
     */
    class DocSequenceSingleSegmentRun extends ValuesReader.Run {
        DocSequenceSingleSegmentRun(Block[] target) {
            super(target);
        }

        void load(int offset) throws IOException {
            initFinalBuilders(offset);
            int shard = docs.shards().getInt(offset);
            int segment = docs.segments().getInt(offset);
            int firstDoc = docs.docs().getInt(offset);
            operator.positionFieldWork(shard, segment, firstDoc);
            LeafReaderContext ctx = operator.ctx(shard, segment);

            moveBuildersAndLoadersToShard();
            currentShard = shard;
            StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
            columnAtATime.clear();
            rowStride.clear();
            for (CurrentWork field : current) {
                field.columnAtATime = field.field.columnAtATime(ctx);
                if (field.columnAtATime != null) {
                    columnAtATime.add(field);
                } else {
                    field.rowStride = field.field.rowStride(ctx);
                    storedFieldsSpec = storedFieldsSpec.merge(field.field.loader.rowStrideStoredFieldSpec());
                    rowStride.add(field);
                }
            }

            ValuesReaderDocs readerDocs = new ValuesReaderDocs(docs);
            storedFields = buildStoredFieldsLoader(storedFieldsSpec, shard, ctx, readerDocs, offset);

            int catStart = offset;
            int p = offset;
            long estimated = 0;
            while (p < docs.getPositionCount() && estimated < operator.jumboBytes) {
                int doc = docs.docs().getInt(p);
                readRowStride(doc);
                p++;

                if (columnAtATime.isEmpty() == false && probeColumnAtATime(catStart, p)) {
                    readColumnAtATimeBatch(catStart, p, readerDocs);
                    catStart = p;
                }

                estimated = estimatedRamBytesUsed() + (long) (p - catStart) * Math.max(0, operator.maxColumnAtATimeBytesPerDoc);
                log.trace("{}: bytes loaded {}/{}", p, estimated, operator.jumboBytes);
            }

            if (columnAtATime.isEmpty() == false && catStart < p) {
                readColumnAtATimeBatch(catStart, p, readerDocs);
            }

            int count = p - offset;
            convertAndAccumulate();
            for (int f = 0; f < target.length; f++) {
                target[f] = deduplicateConstantNull(finalBuilders[f].build());
                assert target[f].getPositionCount() == count : target[f].getPositionCount() + " == " + count + " " + target[f];
                operator.sanityCheckBlock(
                    current[f].columnAtATime != null ? current[f].columnAtATime : current[f].rowStride,
                    count,
                    target[f],
                    f
                );
            }

            if (log.isDebugEnabled()) {
                long actual = 0;
                for (Block b : target) {
                    actual += b.ramBytesUsed();
                }
                log.debug(
                    "loaded {} positions single-segment estimated/actual/jumboBytes {}/{}/{} bytes",
                    count,
                    estimated,
                    actual,
                    operator.jumboBytes
                );
            }
        }

        /**
         * Returns {@code true} when a column-at-a-time probe should fire to
         * bootstrap {@link ValuesSourceReaderOperator#maxColumnAtATimeBytesPerDoc}.
         * Only triggers once, when the per-position rate has not yet been established.
         */
        private boolean probeColumnAtATime(int catStart, int p) {
            return operator.maxColumnAtATimeBytesPerDoc < 0 && p - catStart >= INITIAL_COLUMN_AT_A_TIME_BATCH_SIZE;
        }

        /**
         * Batch-reads column-at-a-time fields for positions {@code [start, end)} and
         * copies the results into the current builders. Since all docs belong to a
         * single segment, the underlying readers advance forward continuously across
         * successive batches. Propagates {@link Block.MvOrdering} from the source
         * blocks and updates {@link ValuesSourceReaderOperator#maxColumnAtATimeBytesPerDoc}.
         */
        private void readColumnAtATimeBatch(int start, int end, ValuesReaderDocs readerDocs) throws IOException {
            if (start >= end) {
                return;
            }
            long before = estimatedRamBytesUsed();
            readerDocs.setCount(end);
            for (CurrentWork c : columnAtATime) {
                assert c.rowStride == null;
                try (Block read = (Block) c.columnAtATime.read(blockFactory, readerDocs, start, c.field.info.nullsFiltered())) {
                    assert read.getPositionCount() == end - start : read.getPositionCount() + " == " + end + " - " + start + " " + read;
                    c.builder.mvOrdering(read.mvOrdering());
                    c.builder.copyFrom(read, 0, read.getPositionCount());
                }
            }
            long batchBytesPerPosition = (estimatedRamBytesUsed() - before) / (end - start);
            operator.maxColumnAtATimeBytesPerDoc = Math.max(operator.maxColumnAtATimeBytesPerDoc, batchBytesPerPosition);
        }
    }

    class DocSequenceRun extends ValuesReader.Run {
        DocSequenceRun(Block[] target) {
            super(target);
        }

        void load(int offset) throws IOException {
            initFinalBuilders(offset);
            int shard = docs.shards().getInt(offset);
            int segment = docs.segments().getInt(offset);
            int firstDoc = docs.docs().getInt(offset);
            operator.positionFieldWork(shard, segment, firstDoc);
            LeafReaderContext ctx = operator.ctx(shard, segment);
            fieldsMoved(ctx, shard);
            int runStart = offset;
            readRowStride(firstDoc);
            int prevDoc = firstDoc;
            int i = offset + 1;
            long estimated = estimatedRamBytesUsed();
            while (i < docs.getPositionCount() && estimated < operator.jumboBytes) {
                int newShard = docs.shards().getInt(i);
                int newSegment = docs.segments().getInt(i);
                int doc = docs.docs().getInt(i);
                if (newShard != shard || newSegment != segment || doc < prevDoc) {
                    readColumnAtATimeBatch(runStart, i);
                    shard = newShard;
                    segment = newSegment;
                    operator.positionFieldWork(shard, segment, doc);
                    ctx = operator.ctx(shard, segment);
                    fieldsMoved(ctx, shard);
                    runStart = i;
                }
                readRowStride(doc);
                prevDoc = doc;
                i++;
                estimated = estimatedRamBytesUsed();
                log.trace("{}: bytes loaded {}/{}", i, estimated, operator.jumboBytes);
            }
            readColumnAtATimeBatch(runStart, i);
            int count = i - offset;
            buildBlocks(count);
            if (log.isDebugEnabled()) {
                long actualBytes = 0;
                for (Block b : target) {
                    actualBytes += b.ramBytesUsed();
                }
                log.debug("loaded {} positions doc sequence estimated/actual {}/{} bytes", count, estimated, actualBytes);
            }
        }

        /**
         * Batch-reads column-at-a-time fields for an ascending run of positions {@code [start, end)}.
         * Within this range doc IDs are non-decreasing, so the underlying doc values iterators
         * can advance forward without needing a reset.
         */
        private void readColumnAtATimeBatch(int start, int end) throws IOException {
            if (columnAtATime.isEmpty() || start >= end) {
                return;
            }
            ValuesReaderDocs readerDocs = new ValuesReaderDocs(docs);
            readerDocs.setCount(end);
            for (CurrentWork c : columnAtATime) {
                assert c.rowStride == null;
                try (Block read = (Block) c.columnAtATime.read(blockFactory, readerDocs, start, c.field.info.nullsFiltered())) {
                    assert read.getPositionCount() == end - start : read.getPositionCount() + " == " + end + " - " + start + " " + read;
                    c.builder.copyFrom(read, 0, read.getPositionCount());
                }
            }
        }

        private void buildBlocks(int count) {
            convertAndAccumulate();
            for (int f = 0; f < target.length; f++) {
                target[f] = deduplicateConstantNull(finalBuilders[f].build());
                assert target[f].getPositionCount() == count : target[f].getPositionCount() + " == " + count + " " + target[f];
                operator.sanityCheckBlock(current[f].rowStride, count, target[f], f);
            }
        }
    }
}
