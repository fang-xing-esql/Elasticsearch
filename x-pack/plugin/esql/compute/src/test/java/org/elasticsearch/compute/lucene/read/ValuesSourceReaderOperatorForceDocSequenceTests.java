/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 * Runs all {@link ValuesSourceReaderOperatorTests} with the doc-sequence threshold
 * set to {@code 0}, forcing {@link ValuesFromDocSequence} for every single or
 * multi-segment page regardless of the number of {@code BytesRef} fields. This
 * validates the correctness of the doc-sequence loading path across all existing
 * test scenarios.
 */
@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS")
public class ValuesSourceReaderOperatorForceDocSequenceTests extends ValuesSourceReaderOperatorTests {

    @Override
    protected int docSequenceBytesRefFieldThreshold() {
        return 0;
    }

    /**
     * {@link ValuesFromDocSequence.DocSequenceSingleSegmentRun} reads column-at-a-time
     * in batches, the first batch probe the first
     * {@link ValuesFromDocSequence#INITIAL_COLUMN_AT_A_TIME_BATCH_SIZE} positions.
     * {@link ColumnAtATimeReaderWithoutReuse} tracks each batch {@code read()} call as a separate
     * reader, so the count depends on the number of batches. With reusable readers the count is 1.
     */

    @Override
    protected Matcher<Integer> expectedSequentialColumnAtATimeReaderCount() {
        return anyOf(equalTo(1), equalTo(2));
    }
}
