/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar InExpression;

fragment IN_EXPRESSION_INIT : [a-z];

//
// IN mode - entered after the IN keyword (see Expression.g4) to decide whether '(' opens a
// subquery or a value list.
// If '(' is followed by a source command keyword (FROM, ROW, etc.), push DEFAULT_MODE for
// subquery parsing. Otherwise, push EXPRESSION_MODE twice (same as regular LP) for value list
// parsing.
// Other tokens are forwarded back to the parser with popMode to preserve error messages.
//
mode IN_MODE;
IN_SUBQUERY_LP : {this.isDevVersion() && this.isNextSourceCommand()}? '(' -> type(LP), popMode, pushMode(DEFAULT_MODE);
IN_LP : '(' -> type(LP), popMode, pushMode(EXPRESSION_MODE), pushMode(EXPRESSION_MODE);

IN_PIPE : '|' -> type(PIPE), popMode, popMode;
IN_OPENING_BRACKET : '[' -> type(OPENING_BRACKET), popMode, pushMode(EXPRESSION_MODE), pushMode(EXPRESSION_MODE);
IN_INTEGER_LITERAL : DIGIT+ -> type(INTEGER_LITERAL), popMode;
IN_DECIMAL_LITERAL
    : (DIGIT+ DOT DIGIT*
    | DOT DIGIT+
    | DIGIT+ (DOT DIGIT*)? EXPONENT
    | DOT DIGIT+ EXPONENT)
    -> type(DECIMAL_LITERAL), popMode
    ;
IN_UNQUOTED_IDENTIFIER
    : (LETTER UNQUOTED_ID_BODY*
    | (UNDERSCORE | ASPERAND) UNQUOTED_ID_BODY+)
    -> type(UNQUOTED_IDENTIFIER), popMode
    ;
IN_QUOTED_IDENTIFIER : QUOTED_ID -> type(QUOTED_IDENTIFIER), popMode;
IN_QUOTED_STRING
    : ('"' (ESCAPE_SEQUENCE | UNESCAPED_CHARS)* '"'
    | '"""' (~[\r\n])*? '"""' '"'? '"'?)
    -> type(QUOTED_STRING), popMode
    ;
IN_PARAM : '?' -> type(PARAM), popMode;

IN_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

IN_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

IN_WS
    : WS -> channel(HIDDEN)
    ;
