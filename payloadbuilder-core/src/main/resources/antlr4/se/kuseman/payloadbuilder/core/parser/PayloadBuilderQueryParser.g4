parser grammar PayloadBuilderQueryParser;

options {
  tokenVocab=PayloadBuilderQueryLexer;
}

@parser::header {
//CSOFF
//@formatter:off
}

query
 : statements
   EOF
 ;

statements
 : (stms+=statement SEMICOLON?)+
 ;

statement
 : miscStatement
 | controlFlowStatement
 | dmlStatement
 | ddlStatement
 ;

miscStatement
 : setStatement
 | useStatement
 | describeStatement
 | analyzeStatement
 | showStatement
 | cacheFlushStatement
 ;

setStatement
 : SET AT? AT? qname EQUALS expression
 ;

useStatement
 : USE qname (EQUALS expression)?
 ;

analyzeStatement
 : ANALYZE selectStatement
 ;

describeStatement
 : DESCRIBE  selectStatement
 ;

showStatement
 : SHOW
 (
      VARIABLES
    | (catalog=identifier HASH)? TABLES
    | (catalog=identifier HASH)? FUNCTIONS
    | CACHES
 )
 ;

cacheFlushStatement
  : CACHE FLUSH cache=fullCacheQualifier key=expression?     #cacheFlush
  | CACHE REMOVE cache=fullCacheQualifier                    #cacheRemove
  ;

fullCacheQualifier
  : type=identifier SLASH (name=cacheName | all=ASTERISK)
  ;

// Cannot use qname above, that causes some mismatched input in antlr
// don't know how to resolve
cacheName
  : parts+=identifier (DOT parts+=identifier)*
  ;

controlFlowStatement
 : ifStatement
 | printStatement
 ;

ifStatement
 : IF condition=expression THEN
   stms=statements
   (ELSE elseStatements=statements)?
   END IF
 ;

printStatement
 : PRINT expression
 ;

dmlStatement
 : selectStatement
 ;

ddlStatement
 : dropTableStatement
 ;

topSelect
 : selectStatement EOF
 ;

selectStatement
 : SELECT (DISTINCT)? (TOP topCount)? selectItem (COMMA selectItem)*
   (INTO into=tableName intoOptions=tableSourceOptions?)?
   (FROM tableSourceJoined)?
   (WHERE where=expression)?
   (GROUP BY groupBy+=expression (COMMA groupBy+=expression)*)?
   (HAVING having=expression)?
   (ORDER BY sortItem (COMMA sortItem)*)?
   (forClause)?
 ;

forClause
 : FOR function=functionName
 ;

dropTableStatement
 : DROP TABLE (IF EXISTS)? tableName
 ;

topCount
 : NUMERIC_LITERAL
 | PARENO expression PARENC
 ;

selectItem
 :
    (alias=identifier DOT)? ASTERISK
 |  (variable EQUALS)? expression (AS? identifier)?
 ;

tableSourceJoined
 : tableSource joinPart*
 ;

 // Nested table sources always require an identifier
 // tableName and tableFunction don't require when used single handedly without joins
 // so those are verified after parsing
tableSource
 : tableName                                    identifier? tableSourceOptions?
 | functionCall                                 identifier? tableSourceOptions?
 | variable                                     identifier?
 | PARENO (selectStatement | expression) PARENC identifier  tableSourceOptions?
 ;

tableSourceOptions
 : WITH PARENO options+=tableSourceOption (COMMA options+=tableSourceOption)* PARENC
 ;

tableSourceOption
 : qname EQUALS expression
 ;

tableName
 : (catalog=identifier HASH)? qname
 | tempHash=HASH qname
 ;

joinPart
 : (INNER | LEFT | RIGHT) POPULATE? JOIN  tableSource ON expression
 | CROSS                  POPULATE? JOIN  tableSource
 | (CROSS | OUTER)        POPULATE? APPLY tableSource
;

sortItem
 : expression order=(ASC | DESC)?
   (NULLS nullOrder=(FIRST | LAST))?
 ;

// ---------------------- Expressions ----------------------------------

topExpression
 : expression EOF
 ;

/*
    Precedence of operators (higest to lowest, inspired from postgres parser)
    .                               left    Dereference identifier / chain function calls
    [ ]                             left    array element selection
    + -                             right   unary plus, unary minus
    ^                               left    exponentiation
    * / %                           left    multiplication, division, modulo
    + -                             left    addition, subtraction
    BETWEEN IN LIKE ILIKE SIMILAR           range containment, set membership, string matching
    < > = <= >= <>                          comparison operators
    IS ISNULL NOTNULL                       IS TRUE, IS FALSE, IS NULL, IS DISTINCT FROM, etc
    NOT                             right   logical negation
    AND                             left    logical conjunction
    OR                              left    logical disjunction

    Note! The rules starting from expression is in opposite order according
    to precedence
*/

expression
 : expr_or
 ;

expr_or
 : left=expr_and (OR right+=expr_and)*
 ;
expr_and
 : left=expr_unary_not (AND right+=expr_unary_not)*
 ;

expr_unary_not
 : NOT* expr_is_not_null
 ;

expr_is_not_null
 : expr_compare (IS NOT? NULL)?
 ;

expr_compare
 : left=expr_in (op=(EQUALS | NOTEQUALS | LESSTHAN | LESSTHANEQUAL | GREATERTHAN| GREATERTHANEQUAL) right=expr_in)?
 ;

expr_in
 : left=expr_like (NOT? IN PARENO expr_list PARENC)?
 ;
expr_like
 : left=expr_add (NOT? LIKE right=expr_add (ESCAPE escape=expression)?)?
 ;

// expr_between
//  :
//  ;

expr_add
 : left=expr_mul (op+=(MINUS | PLUS) right+=expr_mul)*
 ;

expr_mul
 : left=expr_unary_sign (op+=(ASTERISK | SLASH | PERCENT) right+=expr_unary_sign)*
 ;

//expr_pow
// :
// ;

expr_unary_sign
 : op=(MINUS | PLUS)? expr_at_time_zone
 ;

expr_at_time_zone
 : value=primary (AT_WORD TIME ZONE expression)?
 ;

primary
 : qname indirection*                                          #columnReference
 | literal                                                     #literalExpression
 | variable indirection*                                       #variableExpression
 | bracket_expression indirection*                             #bracketExpression
 | CASE when+ (ELSE elseExpr=expression)? END                  #caseExpression
 | scalarFunctionCall indirection*                             #functionCallExpression
 | identifier ARROW expression                                 #lambdaExpression
 | PARENO identifier (COMMA identifier)+ PARENC ARROW expression
                                                               #lambdaExpression
 ;

// Chaining of functions, subcripts etc.
indirection
 : DOT (identifier | scalarFunctionCall)
 | (BRACKETO expression BRACKETC)
 ;

expr_list
 : expression (COMMA expression)*
 ;

bracket_expression
 : PARENO expression PARENC
 | PARENO selectStatement PARENC
 ;

when
 : WHEN condition=expression THEN result=expression
 ;

scalarFunctionCall
 : COUNT PARENO (ALL | DISTINCT)? (ASTERISK | arg=expression) PARENC                                                    #countExpression
 | CAST PARENO input=expression (AS dataType=IDENTIFIER | COMMA arg=expression) PARENC                                  #castExpression
 | DATEADD  PARENO (datepart=IDENTIFIER | datepartE=expression) COMMA number=expression COMMA date=expression PARENC    #dateAddExpression
 | DATEPART PARENO (datepart=IDENTIFIER | datepartE=expression) COMMA date=expression PARENC                            #datePartExpression
 | DATEDIFF PARENO datepart=IDENTIFIER COMMA start=expression COMMA end=expression PARENC                               #dateDiffExpression
 | functionCall                                                                                                         #genericFunctionCallExpression
 ;

functionCall
 : functionName PARENO (ALL | DISTINCT)? (arguments+=functionArgument (COMMA arguments+=functionArgument)*)? PARENC
 ;

functionArgument
 : (name=identifier COLON)? arguments+=expression
 ;

functionName
 : (catalog=identifier HASH)? function=identifier
 ;

literal
 : NULL
 | booleanLiteral
 | numericLiteral
 | decimalLiteral
 | stringLiteral
 | templateStringLiteral
 ;

templateStringLiteral
    : BACKTICK templateStringAtom* BACKTICK
    ;

templateStringAtom
    : TEMPLATESTRINGATOM
    | TEMPLATESTRINGSTARTEXPRESSION expression BRACEC
    ;

variable
 : AT (system=AT)? qname
 ;

compareOperator
 : EQUALS
 | NOTEQUALS
 | LESSTHAN
 | LESSTHANEQUAL
 | GREATERTHAN
 | GREATERTHANEQUAL
 ;

qname
 : parts+=identifier (DOT parts+=identifier)*
 ;

identifier
 : IDENTIFIER
 | QUOTED_IDENTIFIER
 | nonReserved
 ;

numericLiteral
 : NUMERIC_LITERAL
 ;

decimalLiteral
 : DECIMAL_LITERAL
 ;

stringLiteral
 : STRING
 ;

booleanLiteral
 : TRUE | FALSE
 ;

// These are keywords that are also valid as identifiers
nonReserved
 :
   ALL
 | CACHES
 | COUNT
 | DISTINCT
 | FUNCTIONS
 | LEFT
 | IN
 | POPULATE
 | RIGHT
 | TABLES
 | VARIABLES
 ;
