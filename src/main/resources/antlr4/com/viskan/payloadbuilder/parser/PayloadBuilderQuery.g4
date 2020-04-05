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
 : SELECT selectItem (',' selectItem)*
   (FROM tableSourceJoined)?
   (WHERE where=expression)?
   (GROUPBY groupBy+=expression (',' groupBy+=expression)*)?
   (ORDERBY sortItem (',' sortItem)*)?
   EOF
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
   (ORDERBY sortItem (',' sortItem)*)?
   ')'
 ;
 
tableSourceJoined
 : tableSource joinPart*
 ;

tableSource
 : qname			identifier?
 | functionCall		identifier?
 | populateQuery	identifier?
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
 | functionCall 											#functionCallExpression	
 | identifier '->' expression                               #lambdaExpression
 | '(' identifier (',' identifier)+ ')' '->' expression  	#lambdaExpression
 | value=primary '[' index=expression ']'    				#subscript	
 | qname													#columnReference
 | left=primary '.' (functionCall | qname)					#dereference	
 | '(' expression ')' 										#nestedExpression			
 ;

functionCall
 : qname '(' ( expression (',' expression)*)? ')'
 ;
 
literal
 : NULL														
 | booleanLiteral											
 | numericLiteral											
 | decimalLiteral											
 | stringLiteral											
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
 : identifier ('.' identifier)*
 ;

identifier
 : IDENTIFIER
 | QUOTED_IDENTIFIER
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
 
// Tokens
AND		: A N D;
ARRAY	: A R R A Y;
AS		: A S;
ASC		: A S C;
APPLY	: A P P L Y;
CROSS   : C R O S S;
DESC	: D E S C;
FALSE	: F A L S E;
FIRST	: F I R S T;
FROM	: F R O M;
GROUPBY : G R O U P ' ' B Y;
HAVING  : H A V I N G;
IN		: I N;
INNER	: I N N E R;
IS      : I S;
JOIN	: J O I N;
LAST	: L A S T;
LEFT	: L E F T;
NOT		: N O T;
NULL	: N U L L;
NULLS	: N U L L S;
OBJECT	: O B J E C T;
ON		: O N;
OR		: O R;
ORDERBY	: O R D E R ' ' B Y;
OUTER   : O U T E R;
POPULATE: P O P U L A T E;
SELECT	: S E L E C T;
TRUE	: T R U E;
WHERE	: W H E R E;

ASTERISK			: '*';
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

QUOTED_IDENTIFIER
 : '"' ( ~'"' | '""' )* '"'
 ;

IDENTIFIER
 : (LETTER | '_') (LETTER | DIGIT | '_' | '@')*
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
 
 