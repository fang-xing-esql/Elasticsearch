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
 * Loads values from a single leaf. Much more efficient than {@link ValuesFromManyReader}.
 */
class ValuesFromSingleReader extends ValuesReader {
    private static final Logger log = LogManager.getLogger(ValuesFromSingleReader.class);

    private final int shard;
    private final int segment;

    ValuesFromSingleReader(ValuesSourceReaderOperator operator, DocVector docs) {
        super(operator, docs);
        this.shard = docs.shards().getInt(0);
        this.segment = docs.segments().getInt(0);
        log.debug("initialized {} positions", docs.getPositionCount());
    }

    @Override
    protected void load(Block[] target, int offset) throws IOException {
        if (docs.singleSegmentNonDecreasing()) {
            loadFromSingleLeaf(operator.jumboBytes, target, new ValuesReaderDocs(docs), offset);
            return;
        }
        if (offset != 0) {
            throw new IllegalStateException("can only load partial pages with single-segment non-decreasing pages");
        }
        int[] forwards = docs.shardSegmentDocMapForwards();
        Block[] unshuffled = new Block[target.length];
        try {
            loadFromSingleLeaf(
                Long.MAX_VALUE, // Effectively disable splitting pages when we're not loading in order
                unshuffled,
                new ValuesReaderDocs(docs).mapped(forwards, 0, docs.getPositionCount()),
                0
            );
            final int[] backwards = docs.shardSegmentDocMapBackwards();
            for (int i = 0; i < unshuffled.length; i++) {
                target[i] = unshuffled[i].filter(false, backwards);
                unshuffled[i].close();
                unshuffled[i] = null;
            }
        } finally {
            Releasables.closeExpectNoException(unshuffled);
        }
    }

    private void loadFromSingleLeaf(long jumboBytes, Block[] target, ValuesReaderDocs docs, int offset) throws IOException {
        int firstDoc = docs.get(offset);
        operator.positionFieldWork(shard, segment, firstDoc);
        StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
        LeafReaderContext ctx = operator.ctx(shard, segment);

        List<CurrentWork.ColumnAtATimeWork> columnAtATimeReaders = new ArrayList<>(operator.fields.length);
        List<CurrentWork.RowStrideReaderWork> rowStrideReaders = new ArrayList<>(operator.fields.length);
        try (ComputeBlockLoaderFactory loaderBlockFactory = new ComputeBlockLoaderFactory(operator.driverContext.blockFactory())) {
            for (int f = 0; f < operator.fields.length; f++) {
                ValuesSourceReaderOperator.FieldWork field = operator.fields[f];
                BlockLoader.ColumnAtATimeReader columnAtATime = field.columnAtATime(ctx);
                if (columnAtATime != null) {
                    columnAtATimeReaders.add(new CurrentWork.ColumnAtATimeWork(columnAtATime, field.converter, f));
                } else {
                    rowStrideReaders.add(
                        new CurrentWork.RowStrideReaderWork(
                            field.rowStride(ctx),
                            (Block.Builder) field.loader.builder(loaderBlockFactory, docs.count() - offset),
                            field.converter,
                            f
                        )
                    );
                    storedFieldsSpec = storedFieldsSpec.merge(field.loader.rowStrideStoredFieldSpec());
                }
            }

            if (rowStrideReaders.isEmpty() == false) {
                loadFromRowStrideReaders(jumboBytes, target, storedFieldsSpec, rowStrideReaders, ctx, docs, offset);
            }
            for (CurrentWork.ColumnAtATimeWork r : columnAtATimeReaders) {
                target[r.idx()] = r.convert(
                    (Block) r.reader().read(loaderBlockFactory, docs, offset, operator.fields[r.idx()].info.nullsFiltered())
                );
                operator.sanityCheckBlock(r.reader(), docs.count() - offset, target[r.idx()], r.idx());
            }
            if (log.isDebugEnabled()) {
                long total = 0;
                for (Block b : target) {
                    total += b.ramBytesUsed();
                }
                log.debug("loaded {} positions total ({} bytes)", target[0].getPositionCount(), total);
            }
        } finally {
            Releasables.close(rowStrideReaders);
        }
    }

    private void loadFromRowStrideReaders(
        long jumboBytes,
        Block[] target,
        StoredFieldsSpec storedFieldsSpec,
        List<CurrentWork.RowStrideReaderWork> rowStrideReaders,
        LeafReaderContext ctx,
        ValuesReaderDocs docs,
        int offset
    ) throws IOException {
        BlockLoaderStoredFieldsFromLeafLoader storedFields = buildStoredFieldsLoader(storedFieldsSpec, shard, ctx, docs, offset);
        int p = offset;
        long estimated = 0;
        while (p < docs.count() && estimated < jumboBytes) {
            int doc = docs.get(p++);
            storedFields.advanceTo(doc);
            for (CurrentWork.RowStrideReaderWork work : rowStrideReaders) {
                work.read(doc, storedFields);
            }
            operator.trackSourceBytesAndRelease(storedFields);
            estimated = estimatedRamBytesUsed(rowStrideReaders);
            log.trace("{}: bytes loaded {}/{}", p, estimated, jumboBytes);
        }
        for (CurrentWork.RowStrideReaderWork work : rowStrideReaders) {
            target[work.idx()] = work.build();
            operator.sanityCheckBlock(work.reader(), p - offset, target[work.idx()], work.idx());
        }
        if (log.isDebugEnabled()) {
            long actual = 0;
            for (CurrentWork.RowStrideReaderWork work : rowStrideReaders) {
                actual += target[work.idx()].ramBytesUsed();
            }
            log.debug("loaded {} positions row stride estimated/actual {}/{} bytes", p - offset, estimated, actual);
        }
        docs.setCount(p);
    }
}
