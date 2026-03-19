/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoaderStoredFieldsFromLeafLoader;

import java.io.IOException;
import java.util.List;

/**
 * Work for a single field for the current segment. If there's a conversion, then this contains
 * a "scratch" builder and {@link #convertAndAccumulate} accumulates the scratch builder into
 * the {@link #finalBuilder}. If there isn't a conversion then this accumulates directly into
 * the {@link #finalBuilder} immediately.
 */
class CurrentWork implements Releasable {
    final ValuesSourceReaderOperator.FieldWork field;
    /**
     * Converter for the field at the current segment. It's a copy of
     * {@code field.converter} at the time of construction. By the time we actually
     * go to use the converter, the field has moved onto another shard, changing
     * the value of {@code field.converter}.
     */
    @Nullable
    final ValuesSourceReaderOperator.ConverterEvaluator converter;
    final Block.Builder builder;
    final Block.Builder finalBuilder;

    BlockLoader.ColumnAtATimeReader columnAtATime;
    BlockLoader.RowStrideReader rowStride;

    CurrentWork(
        ComputeBlockLoaderFactory blockFactory,
        DocVector docs,
        ValuesSourceReaderOperator.FieldWork field,
        Block.Builder finalBuilder
    ) {
        this.field = field;
        this.converter = field.converter;
        this.builder = converter == null ? finalBuilder : (Block.Builder) field.loader.builder(blockFactory, docs.getPositionCount());
        this.finalBuilder = finalBuilder;
    }

    void convertAndAccumulate() {
        if (converter == null) {
            return;
        }
        try (Block orig = converter.convert(builder.build())) {
            finalBuilder.copyFrom(orig, 0, orig.getPositionCount());
        }
    }

    @Override
    public void close() {
        if (converter != null) {
            /*
             * If there *isn't* a converter than the `builder` is just the final builder
             * and it's closed by the Run.
             */
            builder.close();
        }
    }

    /**
     * Work for building a column-at-a-time.
     * @param reader reads the values
     * @param idx destination in array of {@linkplain Block}s we build
     */
    record ColumnAtATimeWork(
        BlockLoader.ColumnAtATimeReader reader,
        @Nullable ValuesSourceReaderOperator.ConverterEvaluator converter,
        int idx
    ) {
        Block convert(Block block) {
            return converter == null ? block : converter.convert(block);
        }
    }

    /**
     * Work for row-stride readers.
     * @param reader reads the values
     * @param converter an optional conversion function to apply on load
     * @param idx destination in array of {@linkplain Block}s we build
     */
    record RowStrideReaderWork(
        BlockLoader.RowStrideReader reader,
        Block.Builder builder,
        @Nullable ValuesSourceReaderOperator.ConverterEvaluator converter,
        int idx
    ) implements Releasable {
        void read(int doc, BlockLoaderStoredFieldsFromLeafLoader storedFields) throws IOException {
            reader.read(doc, storedFields, builder);
        }

        Block build() {
            Block result = builder.build();
            return converter == null ? result : converter.convert(result);
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    static long estimatedRamBytesUsed(List<RowStrideReaderWork> rowStrideReaders) {
        long estimated = 0;
        for (RowStrideReaderWork r : rowStrideReaders) {
            estimated += r.builder().estimatedBytes();
        }
        return estimated;
    }
}
