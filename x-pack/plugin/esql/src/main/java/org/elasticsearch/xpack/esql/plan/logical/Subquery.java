/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Subquery extends LeafPlan {

    private final LogicalPlan plan;

    public Subquery(Source source, LogicalPlan plan) {
        super(source);
        this.plan = plan;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public LogicalPlan plan() {
        return plan;
    }

    @Override
    protected NodeInfo<Subquery> info() {
        return NodeInfo.create(this, Subquery::new, plan);
    }

    @Override
    public List<Attribute> output() {
        return plan.output();
    }

    @Override
    public boolean expressionsResolved() {
        // For unresolved expressions to exist in EsRelation is fine, as long as they are not used in later operations
        // This allows for them to be converted to null@unsupported fields in final output, an important feature of ES|QL
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(plan);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Subquery other = (Subquery) obj;
        return Objects.equals(plan, other.plan);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + plan + "]";
    }
}
