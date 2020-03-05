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
 ( ((OBJECT | ARRAY) nestedSelectItem) | expression) (AS? identifier)?
 ;
 
nestedSelectItem
 : '('
   selectItem (',' selectItem)*
   (FROM qname)?
   (WHERE where=expression)?
   ')'
 ;
 
tableSourceJoined
 : tableSource joinItem*
 ;

joinItem
 : populatingJoinPart
 | joinPart
 ;

populatingJoinPart
 : '{'
   joinItem*
   (GROUPBY groupBy+=expression (',' groupBy+=expression)*)?
   (ORDERBY sortItem (',' sortItem)*)?
   (HAVING having=expression)?
   '}'
 ;

tableSource
 : qname			identifier
 | functionCall		identifier
 ;
 
joinPart
 : (INNER | LEFT) JOIN tableSourceJoined ON expression
 | (CROSS | OUTER) APPLY tableSourceJoined
;

sortItem
 : expression order=(ASC | DESC)? 
   (NULLS nullOrder=(FIRST | LAST))?						
 ;
 
// Expressions
 
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
AND		: A N D;//'and';
ARRAY	: A R R A Y;//'array';
AS		: A S;//'as';
ASC		: A S C;//'asc';
APPLY	: A P P L Y;//'apply';
CROSS   : C R O S S;//'cross';
DESC	: D E S C;//'desc';
FALSE	: F A L S E;//'false';
FIRST	: F I R S T;//'first';
FROM	: F R O M;//'from';
GROUPBY : G R O U P ' ' B Y;//'group by';
HAVING  : H A V I N G;
IN		: I N;//'in';
INNER	: I N N E R;//'inner';
IS      : I S;//'is';
JOIN	: J O I N;//'join';
LAST	: L A S T;//'last';
LEFT	: L E F T;//'left';
NOT		: N O T;//'not';
NULL	: N U L L;//'null';
NULLS	: N U L L S;//'nulls';
OBJECT	: O B J E C T;//'object';
ON		: O N;//'on';
OR		: O R;//'or';
ORDERBY	: O R D E R ' ' B Y;//'order by';
OUTER   : O U T E R;//'outer';
SELECT	: S E L E C T;//'select';
TRUE	: T R U E;//'true';
WHERE	: W H E R E;//'where';

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
 : ['] ( ~['\r\n\\] | '\\' ~[\r\n] )* [']
 ;

IDENTIFIER
 : (LETTER | '_') (LETTER | DIGIT | '_' | '@')*
 ;

QUOTED_IDENTIFIER
 : '"' ( ~'"' | '""' )* '"'
 | '\'' (~'\'' | '\'\'')* '\''
 ;
 
Comment
 : ( '//' ~[\r\n]* | '/*' .*? '*/' ) -> skip
 ;
 
Space
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
 
 