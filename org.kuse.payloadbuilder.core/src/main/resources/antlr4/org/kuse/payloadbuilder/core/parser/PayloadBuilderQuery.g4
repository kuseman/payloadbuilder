grammar PayloadBuilderQuery;
@lexer::header {
//CSOFF
//@formatter:off
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
 : (stms+=statement ';'?)+
 ;
 
statement
 : miscStatement
 | controlFlowStatement
 | dmlStatement
 ;
 
miscStatement
 : setStatement
 | useStatement
 | describeStatement
 | showStatement
 ;

setStatement
 : SET '@' qname EQUALS expression
 ;
 
useStatement
 : USE qname (EQUALS expression)?
 ;
 
describeStatement
 : DESCRIBE 
 (
 	 tableName 
   | selectStatement 
 )
 ;

showStatement
 : SHOW 
 (
 	  VARIABLES 
 	| (catalog=identifier '#')? TABLES 
 	| (catalog=identifier '#')? FUNCTIONS
 )
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

topSelect
 : selectStatement EOF
 ;

selectStatement
 : SELECT (TOP topCount)? selectItem (',' selectItem)*
   (FROM tableSourceJoined)?
   (WHERE where=expression)?
   (GROUPBY groupBy+=expression (',' groupBy+=expression)*)?
   (ORDERBY sortItem (',' sortItem)*)?
 ;

topCount
 : NUMERIC_LITERAL
 | '(' expression ')'
 ;

selectItem
 : 
 	ASTERISK
 |  alias=identifier '.' ASTERISK
 |  (OBJECT | ARRAY) nestedSelectItem (AS? identifier)?
 |  (variable EQUALS)? expression 	  (AS? identifier)?
 ;
 
nestedSelectItem
 : '('
   selectItem (',' selectItem)*
   (FROM from=expression)?
   (WHERE where=expression)?
   (GROUPBY groupBy+=expression (',' groupBy+=expression)*)?
   (ORDERBY orderBy+=sortItem (',' orderBy+=sortItem)*)?
   ')'
 ;
 
tableSourceJoined
 : tableSource joinPart*
 ;

tableSource
 : tableName			identifier? tableSourceOptions?
 | functionCall			identifier? tableSourceOptions?
 | subQuery				identifier? tableSourceOptions?
 ;

tableSourceOptions
 : WITH '(' options+=tableSourceOption (',' tableOptions+=tableSourceOption)* ')'
 ;

tableSourceOption
 : qname EQUALS expression
 ;
 
tableName
 : (catalog=identifier '#')? qname
 ;

joinPart
 : (INNER | LEFT) JOIN tableSource ON expression
 | (CROSS | OUTER) APPLY tableSource
;

subQuery
 : '('
//   (SELECT (TOP topCount)? selectItem (',' selectItem)*)? TODO: build a custom select with items
   FROM tableSourceJoined
   (WHERE where=expression)?
   (GROUPBY groupBy+=expression (',' groupBy+=expression)*)?
   (ORDERBY sortItem (',' sortItem)*)?
   ')'
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
 | left=expression
   op=(EQUALS | NOTEQUALS| LESSTHAN | LESSTHANEQUAL | GREATERTHAN| GREATERTHANEQUAL) 
   right=expression											#comparisonExpression

 //
 
 | left=expression 
   NOT? IN 
   '(' expression (',' expression)* ')'						#inExpression
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
 | left=primary '.' (identifier | functionCall)				#dereference
 | identifier												#columnReference
 | functionCall 											#functionCallExpression	
 | identifier '->' expression								#lambdaExpression
 | '(' identifier (',' identifier)+ ')' '->' expression  	#lambdaExpression
 | value=primary '[' subscript=expression ']'    			#subscript
 | variable													#variableExpression
 | '(' expression ')' 										#nestedExpression
 | CASE when+ (ELSE elseExpr=expression)? END               #caseExpression
 ;

when
 : WHEN condition=expression THEN result=expression
 ;

functionCall
 : functionName '(' ( arguments+=functionArgument (',' arguments+=functionArgument)*)? ')'
 ;
 
functionArgument
 : (name=identifier ':')? arguments+=expression
 ;
 
functionName
 : (catalog=identifier '#')? function=identifier
 ;
 
literal
 : NULL
 | booleanLiteral
 | numericLiteral
 | decimalLiteral
 | stringLiteral
 ;
 
variable
 : '@' (system='@')? qname
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
 : parts+=identifier ('.' parts+=identifier)*
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
 : FROM | FIRST | TABLES | LIKE
 ;
 
// Tokens

AND		     : A N D;
ARRAY	     : A R R A Y;
AS		     : A S;
ASC		     : A S C;
APPLY	     : A P P L Y;
CASE         : C A S E;
CROSS        : C R O S S;
DESC	     : D E S C;
DESCRIBE	 : D E S C R I B E;
ELSE		 : E L S E;
END			 : E N D;
ESCAPE		 : E S C A P E;
FALSE	     : F A L S E;
FIRST	     : F I R S T;
FROM	     : F R O M;
FUNCTIONS	 : F U N C T I O N S;
GROUPBY      : G R O U P ' ' B Y;
HAVING       : H A V I N G;
IF           : I F;
IN		     : I N;
INNER	     : I N N E R;
IS           : I S;
JOIN	     : J O I N;
LAST	     : L A S T;
LEFT	     : L E F T;
LIKE		 : L I K E;
NOT		     : N O T;
NULL	     : N U L L;
NULLS	     : N U L L S;
OBJECT	     : O B J E C T;
ON		     : O N;
OR		     : O R;
ORDERBY	     : O R D E R ' ' B Y;
OUTER        : O U T E R;
PARAMETERS   : P A R A M E T E R S;
PRINT        : P R I N T;
SELECT	     : S E L E C T;
SESSION		 : S E S S I O N;
SET			 : S E T;
SHOW		 : S H O W;
TABLES		 : T A B L E S;
THEN		 : T H E N;
TOP			 : T O P;
TRUE	     : T R U E;
USE			 : U S E;
VARIABLES	 : V A R I A B L E S;
WHEN		 : W H E N;
WITH         : W I T H;
WHERE	     : W H E R E;

ASTERISK			: '*';
COLON				: ':';
EQUALS				: '=';
EXCLAMATION			: '!';
GREATERTHAN			: '>';
GREATERTHANEQUAL	: '>=';
LESSTHAN			: '<';
LESSTHANEQUAL		: '<=';
MINUS				: '-';
NOTEQUALS			: '!=';
PERCENT				: '%';
PLUS				: '+';
SLASH				: '/';

// From java-grammar
NUMERIC_LITERAL
 : ('0' | [1-9] (DIGITS? | '_'+ DIGITS)) [lL]?
 ;
 
// From java-grammar 
DECIMAL_LITERAL
 : (DIGITS '.' DIGITS? | '.' DIGITS) EXPONENTPART? [fFdD]?
 |  DIGITS (EXPONENTPART [fFdD]? | [fFdD])
 ;
 
STRING
 : '\'' ( ~'\'' | '\'\'' )* '\''
 ;

IDENTIFIER
 : (LETTER | '_') (LETTER | DIGIT | '_')*
 ;

QUOTED_IDENTIFIER
 : '"' ( ~'"' | '""' )* '"'
 ;

LINE_COMMENT
 : '--' ~[\r\n]* '\r'? '\n'? -> skip
 ;

BLOCK_COMMENT
 : ( '//' ~[\r\n]* | '/*' .*? '*/' ) -> skip
 ;
 
SPACE
 : [ \t\r\n\u000C] -> skip
 ;
 
fragment LETTER
 : [A-Za-z]
 ;
 
fragment DIGIT 
 : [0-9]
 ;
 
fragment DIGITS
 : [0-9] ([0-9_]* [0-9])?
 ;

fragment EXPONENTPART
 : [eE] [+-]? DIGITS
 ;
 
fragment A : [aA]; 
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
 
 