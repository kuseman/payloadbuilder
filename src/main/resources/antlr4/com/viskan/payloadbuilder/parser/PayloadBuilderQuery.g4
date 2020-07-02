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
 : (statement ';'?)+
 ;
 
statement
 : miscStatement
 | controlFlowStatement
 | dmlStatement
 ;
 
miscStatement
 : setStatement
 ;

setStatement
 : SET qname EQUALS expression
 ;

controlFlowStatement
 : ifStatement
 | printStatement
 ;

ifStatement
 : IF condition=expression THEN
   statements
   (ELSE elseStatements+=statements)?
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
 : SELECT selectItem (',' selectItem)*
   (FROM tableSourceJoined)?
   (WHERE where=expression)?
   (GROUPBY groupBy+=expression (',' groupBy+=expression)*)?
   (ORDERBY sortItem (',' sortItem)*)?
 ;

selectItem
 : 
 (((OBJECT | ARRAY) nestedSelectItem) | expression) (AS? identifier)?
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
 : qname				identifier? (WITH '(' tableOptions+=table_with_option (',' tableOptions+=table_with_option)* ')' )?
 | functionCall			identifier?
 | populateQuery		identifier?
 ;

table_with_option
 : BATCH_SIZE EQUALS size=NUMBER					#batchSize
 ;

joinPart
 : (INNER | LEFT) JOIN tableSource ON expression
 | (CROSS | OUTER) APPLY tableSource
;

populateQuery
 : '['
   tableSourceJoined
   (WHERE where=expression)?
   (GROUPBY groupBy+=expression (',' groupBy+=expression)*)?
   (ORDERBY sortItem (',' sortItem)*)?
   ']'
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
 : primary                                              	#primaryExpression
 | op=(MINUS | PLUS) expression								#arithmeticUnary 
 | left=expression 
   op=(ASTERISK | SLASH | PERCENT | PLUS | MINUS)
   right=expression                                         #arithmeticBinary
 | left=expression
   op=(EQUALS | NOTEQUALS| LESSTHAN | LESSTHANEQUAL | GREATERTHAN| GREATERTHANEQUAL) 
   right=expression											#comparisonExpression
 | left=expression 
   NOT? IN 
   '(' expression (',' expression)* ')'						#inExpression
 | expression IS NOT? NULL  								#nullPredicate
 | NOT expression                                           #logicalNot
 | left=expression 
   op=(AND | OR) 
   right=expression     									#logicalBinary
 ;
 
primary
 : literal													#literalExpression
 | left=primary '.' (identifier | functionCall)				#dereference	
 | identifier												#columnReference
 | functionCall 											#functionCallExpression	
 | identifier '->' expression                               #lambdaExpression
 | '(' identifier (',' identifier)+ ')' '->' expression  	#lambdaExpression
 | value=primary '[' index=expression ']'    				#subscript	
 | namedParameter											#namedParameterExpression
 | '(' expression ')' 										#nestedExpression			
 ;

functionCall
 : qname '(' ( arguments+=expression (',' arguments+=expression)*)? ')'
 ;
 
literal
 : NULL														
 | booleanLiteral											
 | numericLiteral											
 | decimalLiteral											
 | stringLiteral											
 ;
 
namedParameter
 : COLON identifier
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
 : (catalog=identifier '#')? parts+=identifier ('.' parts+=identifier)*
 ;

identifier
 : IDENTIFIER
 | QUOTED_IDENTIFIER
 | nonReserved
 ;
 
numericLiteral
 : NUMBER
 ;
 
decimalLiteral
 : DECIMAL
 ;

stringLiteral
 : STRING 
 ;

booleanLiteral
 : TRUE | FALSE 
 ;
 
nonReserved
 : FROM | FIRST
 ;
 
// Tokens

AND		     : A N D;
ARRAY	     : A R R A Y;
AS		     : A S;
ASC		     : A S C;
APPLY	     : A P P L Y;
BATCH_SIZE   : B A T C H '_' S I Z E;
CROSS        : C R O S S;
DESC	     : D E S C;
ELSE		 : E L S E;
END			 : E N D;
FALSE	     : F A L S E;
FIRST	     : F I R S T;
FROM	     : F R O M;
GROUPBY      : G R O U P ' ' B Y;
HAVING       : H A V I N G;
IF           : I F;
IN		     : I N;
INNER	     : I N N E R;
IS           : I S;
JOIN	     : J O I N;
LAST	     : L A S T;
LEFT	     : L E F T;
NOT		     : N O T;
NULL	     : N U L L;
NULLS	     : N U L L S;
OBJECT	     : O B J E C T;
ON		     : O N;
OR		     : O R;
ORDERBY	     : O R D E R ' ' B Y;
OUTER        : O U T E R;
PRINT        : P R I N T;
SELECT	     : S E L E C T;
SET			 : S E T;
THEN		 : T H E N;
TRUE	     : T R U E;
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

NUMBER
 : DIGIT+
 ;

DECIMAL
 : DIGIT+ '.' DIGIT*
 | '.' DIGIT+
 ;

STRING
 : '\'' ( ~'\'' | '\'\'' )* '\''
 ;

IDENTIFIER
 : (LETTER | '_') (LETTER | DIGIT | '_' | '@')*
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
 
 