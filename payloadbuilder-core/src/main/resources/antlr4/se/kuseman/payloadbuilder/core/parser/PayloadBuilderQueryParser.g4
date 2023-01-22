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
 : SET AT? qname EQUALS expression
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
  : CACHE FLUSH cache=fullCacheQualifier key=expression?  #cacheFlush
  | CACHE REMOVE cache=fullCacheQualifier  				  #cacheRemove
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
   (GROUPBY groupBy+=expression (COMMA groupBy+=expression)*)?
   (HAVING having=expression)?
   (ORDERBY sortItem (COMMA sortItem)*)?
   (forClause)?
 ;

forClause
 : FOR function=functionName (OVER alias=identifier)?
 ;

dropTableStatement
 : DROP TABLE (IF EXISTS)? tableName
 ;

topCount
 : NUMERIC_LITERAL
 | '(' expression ')'
 ;

selectItem
 :
	(alias=identifier DOT)? ASTERISK
 |  (variable EQUALS)? expression (AS? identifier)?
 ;

tableSourceJoined
 : tableSource joinPart*
 ;

tableSource
 : tableName						identifier? tableSourceOptions?
 | functionCall						identifier? tableSourceOptions?
 | PARENO selectStatement PARENC	identifier? tableSourceOptions?
 ;

tableSourceOptions
 : WITH '(' options+=tableSourceOption (COMMA options+=tableSourceOption)* ')'
 ;

tableSourceOption
 : qname EQUALS expression
 ;

tableName
 : (catalog=identifier HASH)? qname
 | tempHash=HASH qname
 ;

joinPart
 : (INNER | LEFT | RIGHT | CROSS) POPULATE? JOIN tableSource (ON expression)?
 | (CROSS | OUTER) POPULATE? APPLY tableSource
;

sortItem
 : expression order=(ASC | DESC)?
   (NULLS nullOrder=(FIRST | LAST))?
 ;

// Expressions

topExpression
 : expression EOF
 ;

expression
 : primary													#primaryExpression

 //

 | op=(MINUS | PLUS) expression								#arithmeticUnary
 | left=expression
   op=(ASTERISK | SLASH | PERCENT | PLUS | MINUS)
   right=expression											#arithmeticBinary
   
 | expression timeZone                                      #atTimeZoneExpression
   
 | left=expression
   op=(EQUALS | NOTEQUALS| LESSTHAN | LESSTHANEQUAL | GREATERTHAN| GREATERTHANEQUAL)
   right=expression											#comparisonExpression

 //

 | left=expression
   NOT? IN
   '(' expression (COMMA expression)* ')'					#inExpression
 | left=expression
   // Have to use primary here to solve ambiguity when ie. nesting AND's
   NOT? LIKE right=primary
   (ESCAPE escape=expression)?								#likeExpression
 | expression IS NOT? NULL  								#nullPredicate

 //

 | NOT expression											#logicalNot
 | left=expression
   op=(AND | OR)
   right=expression											#logicalBinary
 ;

primary
 : literal													#literalExpression
 | left=primary DOT (identifier | functionCall)				#dereference
 | qname													#columnReference
 | builtInFunctionCall                                      #builtInFunctionCallExpression
 | functionCall 											#functionCallExpression
 | identifier ARROW expression								#lambdaExpression
 | PARENO identifier (COMMA identifier)+ PARENC ARROW expression
                                                            #lambdaExpression
 | value=primary BRACKETO subscript=expression BRACKETC		#subscript
 | variable													#variableExpression
 | bracket_expression 										#bracketExpression
 | CASE when+ (ELSE elseExpr=expression)? END               #caseExpression
 ;

bracket_expression
 : PARENO expression PARENC
 | PARENO selectStatement PARENC
 ;

when
 : WHEN condition=expression THEN result=expression
 ;

builtInFunctionCall
 : CAST PARENO input=expression (AS dataType=IDENTIFIER | COMMA arg=expression) PARENC                                  #castExpression
 | DATEADD  PARENO (datepart=IDENTIFIER | datepartE=expression) COMMA number=expression COMMA date=expression PARENC    #dateAddExpression
 | DATEPART PARENO (datepart=IDENTIFIER | datepartE=expression) COMMA date=expression PARENC                            #datePartExpression
 | DATEDIFF PARENO datepart=IDENTIFIER COMMA start=expression COMMA end=expression PARENC                               #dateDiffExpression
 ;
 
 timeZone
 : AT_WORD TIME ZONE expression
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

nonReserved
 : FROM
 | FIRST
 | FUNCTIONS
 | TABLE
 | TABLES
 | LIKE
 | FOR
 | ALL
 | CACHE
 | CACHES
 | FLUSH
 | REMOVE
 | VARIABLES
 | POPULATE
 | DISTINCT
 ;
