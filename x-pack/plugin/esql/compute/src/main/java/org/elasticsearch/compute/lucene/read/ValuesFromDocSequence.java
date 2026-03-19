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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoaderStoredFieldsFromLeafLoader;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.compute.lucene.read.CurrentWork.estimatedRamBytesUsed;

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
 *     For single-segment non-decreasing pages, {@link DocSequenceSingleSegmentNonDecreasingRun}
 *     reads row-stride fields in a main loop with a jumbo-bytes check for partial
 *     page support. Column-at-a-time fields are read once after the loop, for the
 *     same range of positions. When only column-at-a-time fields are present, all
 *     positions in the first page are read at once (no jumboBytes splitting), matching
 *     the loading strategy of {@link ValuesFromSingleReader}, the subsequent pages are
 *     read with a jumbo-bytes check for partial page support according to the average
 *     column-at-a-time reader size calculated based on the first page.
 * </p>
 */
class ValuesFromDocSequence extends ValuesReader {
    private static final Logger log = LogManager.getLogger(ValuesFromDocSequence.class);

    ValuesFromDocSequence(ValuesSourceReaderOperator operator, DocVector docs) {
        super(operator, docs);
        log.debug("initializing {} positions", docs.getPositionCount());
    }

    @Override
    protected void load(Block[] target, int offset) throws IOException {
        if (docs.singleSegmentNonDecreasing()) {
            try (DocSequenceSingleSegmentNonDecreasingRun run = new DocSequenceSingleSegmentNonDecreasingRun(target)) {
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
     * are non-decreasing within a single segment, column-at-a-time readers can
     * advance forward continuously.
     * <p>
     *     Row-stride fields are read in a main loop with a jumbo-bytes check for
     *     partial page support; the jumbo estimate includes column-at-a-time bytes
     *     per doc so that both field kinds contribute to page-size control.
     *     Column-at-a-time fields are then read once for the same position range.
     * </p>
     * <p>
     *     When only column-at-a-time fields are present, all positions in the first
     *     page are read at once without jumboBytes splitting. the subsequent pages are
     *     read with a jumbo-bytes check for partial page support according to the average
     *     column-at-a-time reader size calculated based on the first page.
     * </p>
     * <p>
     *     Also supports sequential stored field readers (like {@link ValuesFromSingleReader}).
     * </p>
     */
    class DocSequenceSingleSegmentNonDecreasingRun implements Releasable {
        private final ComputeBlockLoaderFactory blockFactory;
        final Block[] target;

        DocSequenceSingleSegmentNonDecreasingRun(Block[] target) {
            this.target = target;
            this.blockFactory = new ComputeBlockLoaderFactory(operator.driverContext.blockFactory());
        }

        void load(int offset) throws IOException {
            int shard = docs.shards().getInt(offset);
            int segment = docs.segments().getInt(offset);
            ValuesReaderDocs readerDocs = new ValuesReaderDocs(docs);
            operator.positionFieldWork(shard, segment, readerDocs.get(offset));
            LeafReaderContext ctx = operator.ctx(shard, segment);

            List<CurrentWork.ColumnAtATimeWork> columnAtATimeReaders = new ArrayList<>(operator.fields.length);
            List<CurrentWork.RowStrideReaderWork> rowStrideReaders = new ArrayList<>(operator.fields.length);
            StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
            try {
                for (int f = 0; f < operator.fields.length; f++) {
                    ValuesSourceReaderOperator.FieldWork field = operator.fields[f];
                    BlockLoader.ColumnAtATimeReader cat = field.columnAtATime(ctx);
                    if (cat != null) {
                        columnAtATimeReaders.add(new CurrentWork.ColumnAtATimeWork(cat, field.converter, f));
                    } else {
                        rowStrideReaders.add(
                            new CurrentWork.RowStrideReaderWork(
                                field.rowStride(ctx),
                                (Block.Builder) field.loader.builder(blockFactory, docs.getPositionCount() - offset),
                                field.converter,
                                f
                            )
                        );
                        storedFieldsSpec = storedFieldsSpec.merge(field.loader.rowStrideStoredFieldSpec());
                    }
                }

                int p = offset;
                long estimated = 0;
                if (rowStrideReaders.isEmpty() == false) {
                    BlockLoaderStoredFieldsFromLeafLoader storedFields = buildStoredFieldsLoader(
                        storedFieldsSpec,
                        shard,
                        ctx,
                        readerDocs,
                        offset
                    );
                    // Read row by row first. Column-at-a-time bytes are included in the jumbo
                    // check via maxColumnAtATimeBytesPerDoc.
                    while (p < docs.getPositionCount() && estimated < operator.jumboBytes) {
                        int doc = readerDocs.get(p);
                        storedFields.advanceTo(doc);
                        for (CurrentWork.RowStrideReaderWork r : rowStrideReaders) {
                            r.read(doc, storedFields);
                        }
                        operator.trackSourceBytesAndRelease(storedFields);
                        p++;
                        estimated = estimatedRamBytesUsed(rowStrideReaders) + (long) (p - offset) * Math.max(
                            0,
                            operator.maxColumnAtATimeBytesPerDoc
                        );
                        log.trace("{}: bytes loaded {}/{}", p, estimated, operator.jumboBytes);
                    }
                    readerDocs.setCount(p);
                    for (CurrentWork.RowStrideReaderWork r : rowStrideReaders) {
                        target[r.idx()] = r.build();
                        operator.sanityCheckBlock(r.reader(), p - offset, target[r.idx()], r.idx());
                    }
                    readColumnAtATimeBatch(columnAtATimeReaders, readerDocs, offset);
                } else if (columnAtATimeReaders.isEmpty() == false) {
                    // Only column-at-a-time fields: read all positions in the first page at once,
                    // matching ValuesFromSingleReader behavior. jumboBytes splitting starts from
                    // the subsequent pages after the average ColumnAtATime size is populated.
                    while (p < docs.getPositionCount() && estimated < operator.jumboBytes) {
                        p++;
                        estimated = (long) (p - offset) * Math.max(0, operator.maxColumnAtATimeBytesPerDoc);
                    }
                    readerDocs.setCount(p);
                    readColumnAtATimeBatch(columnAtATimeReaders, readerDocs, offset);
                }

                int count = readerDocs.count() - offset;
                if (log.isDebugEnabled()) {
                    long actual = 0;
                    for (Block b : target) {
                        actual += b.ramBytesUsed();
                    }
                    log.debug(
                        "loaded {} positions single-segment nonDecreasing estimated/actual/jumboBytes/averageDocSize {}/{}/{}/{} bytes",
                        count,
                        estimated,
                        actual,
                        operator.jumboBytes,
                        operator.maxColumnAtATimeBytesPerDoc
                    );
                }
            } finally {
                Releasables.close(rowStrideReaders);
            }
        }

        /**
         * Reads all column-at-a-time fields for positions {@code [offset, readerDocs.count())} and
         * sets their entries in {@link #target}. Also updates
         * {@link ValuesSourceReaderOperator#maxColumnAtATimeBytesPerDoc} from the actual bytes loaded.
         *
         * Different from DocSequenceRun, blocks read from ColumnAtATimeReader are not copied into
         * finalBuilders, as this page has a single segment with non-decreasing docIds.
         */
        private void readColumnAtATimeBatch(List<CurrentWork.ColumnAtATimeWork> catList, ValuesReaderDocs readerDocs, int offset)
            throws IOException {
            int count = readerDocs.count() - offset;
            for (CurrentWork.ColumnAtATimeWork c : catList) {
                Block block = (Block) c.reader().read(blockFactory, readerDocs, offset, operator.fields[c.idx()].info.nullsFiltered());
                assert block.getPositionCount() == count : block.getPositionCount() + " == " + count + " " + block;
                target[c.idx()] = c.convert(block);
                operator.sanityCheckBlock(c.reader(), count, target[c.idx()], c.idx());
            }
            if (count > 0) {
                long catBytes = 0;
                for (CurrentWork.ColumnAtATimeWork c : catList) {
                    catBytes += target[c.idx()].ramBytesUsed();
                }
                operator.maxColumnAtATimeBytesPerDoc = Math.max(operator.maxColumnAtATimeBytesPerDoc, catBytes / count);
            }
        }

        @Override
        public void close() {
            blockFactory.close();
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
                log.debug(
                    "loaded {} positions doc sequence estimated/actual/jumboBytes {}/{}/{} bytes",
                    count,
                    estimated,
                    actualBytes,
                    operator.jumboBytes
                );
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
