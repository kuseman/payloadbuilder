grammar PayloadBuilderQuery;
@lexer::header {
//CSOFF
//@formatter:off
package com.viskan.payloadbuilder.parser;
}
@parser::header {
//CSOFF
//@formatter:off
package com.viskan.payloadbuilder.parser;
}

query
 : SELECT selectItem (COMMA selectItem)*
   FROM from=qname alias=identifier
   (WHERE where=expression)?
   (ORDERBY sortItem (COMMA sortItem)*)?
   EOF
 ;

selectItem
 : 
 ( ((OBJECT | ARRAY) nestedSelectItem) | expression) (AS? identifier)?
// | qname DOT Asterix
// | Asterix
 ;
 
nestedSelectItem
 : OPAREN
   selectItem (COMMA selectItem)*
   (FROM from=qname)?
   (WHERE where=expression)?
   CPAREN
 ;
 
//join
// : type=(INNER | LEFT) JOIN qname identifier
//   ((populatingJoin, (populatingJoin)| ON expression
// ;
//
//populatingJoin
// : '('
//     
//   ')'
// ;
//
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
   OPAREN expression (COMMA expression)* CPAREN				#inExpression
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
 | left=primary DOT (functionCall | qname)					#dereference	
 | '(' expression ')' 										#nestedExpression			
 ;

functionCall
 : qname OPAREN ( expression (COMMA expression)*)? CPAREN
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
 : identifier (DOT identifier)*
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
 
// Common tokens 
OPAREN: '(';
CPAREN: ')';
 
AND		: 'and';
ARRAY	: 'array';
AS		: 'as';
ASC		: 'asc';
COMMA	: ',';
DESC	: 'desc';
DOT		: '.';
EXISTS	: 'exists';
FALSE	: 'false';
FIRST	: 'first';
FROM	: 'from';
IN		: 'in';
INNER	: 'inner';
IS      : 'is';
JOIN	: 'join';
LAST	: 'last';
LEFT	: 'left';
NOT		: 'not';
NULL	: 'null';
NULLS	: 'nulls';
OBJECT	: 'object';
ON		: 'on';
OR		: 'or';
ORDERBY	: 'order by';
SELECT	: 'select';
TRUE	: 'true';
WHERE	: 'where';

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
 