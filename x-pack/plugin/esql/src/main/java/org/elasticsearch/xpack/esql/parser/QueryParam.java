/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represent a strongly typed parameter value
 */
public record QueryParam(String name, Object value, DataType type, ParserUtils.ParamClassification classification) {

    public String nameValue() {
        return "{" + (this.name == null ? "" : this.name + ":") + valueAsString() + "}";
    }

    @Override
    public String toString() {
        return valueAsString() + " [" + name + "][" + type + "][" + classification + "]";
    }

    private String valueAsString() {
        return value instanceof Object[] values ? Arrays.toString(values) : String.valueOf(value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, valueAsString(), type, classification);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        QueryParam other = (QueryParam) obj;

        return Objects.equals(name, other.name)
            && Objects.equals(type, other.type)
            && Objects.equals(classification, other.classification)
            && valueEquals(other.value);
    }

    private boolean valueEquals(Object otherValue) {
        if (this.value instanceof Object[] values) {
            return otherValue instanceof Object[] otherValues && Arrays.equals(values, otherValues);
        } else {
            return (otherValue instanceof Object[]) == false && Objects.equals(this.value, otherValue);
        }
    }
}
