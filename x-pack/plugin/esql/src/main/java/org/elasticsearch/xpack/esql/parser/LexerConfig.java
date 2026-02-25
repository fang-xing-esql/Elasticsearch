/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;

/**
 * Base class for hooking versioning information into the ANTLR parser.
 */
public abstract class LexerConfig extends Lexer {

    // is null when running inside the IDEA plugin
    EsqlConfig config;
    private int promqlDepth = 0;

    public LexerConfig() {}

    public LexerConfig(CharStream input) {
        super(input);
    }

    boolean isDevVersion() {
        return config == null || config.isDevVersion();
    }

    void setEsqlConfig(EsqlConfig config) {
        this.config = config;
    }

    // Needed by the Promql command
    void incPromqlDepth() {
        promqlDepth++;
    }

    void decPromqlDepth() {
        if (promqlDepth == 0) {
            throw new ParsingException("Invalid PromQL command, unexpected '('");
        }
        promqlDepth--;
    }

    void resetPromqlDepth() {
        if (promqlDepth != 0) {
            throw new ParsingException(
                "Invalid PromQL declaration, missing [{}] [{}] parenthesis",
                Math.absExact(promqlDepth),
                promqlDepth > 0 ? '(' : ')'
            );
        }
    }

    boolean isPromqlQuery() {
        return promqlDepth > 0;
    }

    /**
     * Look ahead past '(' and whitespace to check whether the next word is a source command keyword.
     * Used by IN_SUBQUERY_LP in IN_MODE to determine whether '(' starts a subquery (push DEFAULT_MODE) or a value list
     * (push EXPRESSION_MODE).
     */
    boolean isNextSourceCommand() {
        int offset = 2; // LA(1) is '(', start scanning from LA(2)
        while (true) {
            int c = _input.LA(offset);
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                offset++;
            } else {
                break;
            }
        }
        return matchesKeywordAtOffset(offset, "from")
            || matchesKeywordAtOffset(offset, "row")
            || matchesKeywordAtOffset(offset, "show")
            || matchesKeywordAtOffset(offset, "ts");
    }

    private boolean matchesKeywordAtOffset(int offset, String keyword) {
        for (int i = 0; i < keyword.length(); i++) {
            int c = _input.LA(offset + i);
            if (c == -1) {
                return false;
            }
            if (Character.toLowerCase(c) != keyword.charAt(i)) {
                return false;
            }
        }
        int next = _input.LA(offset + keyword.length());
        return next == -1 || (Character.isLetterOrDigit(next) == false && next != '_');
    }
}
