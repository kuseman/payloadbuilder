lexer grammar PayloadBuilderQueryLexer;

options { superClass=PayloadBuilderQueryLexerBase; }

@lexer::header {
//CSOFF
//@formatter:off
}

// Tokens

ALL          : A L L;
AND          : A N D;
ANALYZE      : A N A L Y Z E;
AS           : A S;
AT_WORD      : A T;
ASC          : A S C;
APPLY        : A P P L Y;
CACHE        : C A C H E;
CACHES       : C A C H E S;
CASE         : C A S E;
CAST         : C A S T;
CROSS        : C R O S S;
COUNT        : C O U N T;
DATEADD      : D A T E A D D;
DATEPART     : D A T E P A R T;
DATEDIFF     : D A T E D I F F;
DESC         : D E S C;
DESCRIBE     : D E S C R I B E;
DISTINCT     : D I S T I N C T;
DROP         : D R O P;
ELSE         : E L S E;
FLUSH        : F L U S H;
END          : E N D;
ESCAPE       : E S C A P E;
EXISTS       : E X I S T S;
FALSE        : F A L S E;
FIRST        : F I R S T;
FOR          : F O R;
FROM         : F R O M;
FUNCTIONS    : F U N C T I O N S;
GROUPBY      : G R O U P ' ' B Y;
HAVING       : H A V I N G;
IF           : I F;
IN           : I N;
INNER        : I N N E R;
INSERT       : I N S E R T;
INTO         : I N T O;
IS           : I S;
JOIN         : J O I N;
LAST         : L A S T;
LEFT         : L E F T;
LIKE         : L I K E;
NOT          : N O T;
NULL         : N U L L;
NULLS        : N U L L S;
ON           : O N;
OR           : O R;
ORDERBY      : O R D E R ' ' B Y;
OUTER        : O U T E R;
PARAMETERS   : P A R A M E T E R S;
POPULATE     : P O P U L A T E;
PRINT        : P R I N T;
REMOVE       : R E M O V E;
RIGHT        : R I G H T;
SELECT       : S E L E C T;
SESSION      : S E S S I O N;
SET          : S E T;
SHOW         : S H O W;
TABLE        : T A B L E;
TABLES       : T A B L E S;
THEN         : T H E N;
TIME         : T I M E;
TOP          : T O P;
TRUE         : T R U E;
USE          : U S E;
VARIABLES    : V A R I A B L E S;
WHEN         : W H E N;
WITH         : W I T H;
WHERE        : W H E R E;
ZONE         : Z O N E;

ASTERISK            : '*';
AT                  : '@';
ARROW               : '->';
BRACKETO            : '[';
BRACKETC            : ']';
BRACEC              : {this.isInTemplateString()}? '}' -> popMode;
COLON               : ':';
COMMA               : ',';
DOT                 : '.';
EQUALS              : '=';
EXCLAMATION         : '!';
GREATERTHAN         : '>';
GREATERTHANEQUAL    : '>=';
HASH                : '#';
LESSTHAN            : '<';
LESSTHANEQUAL       : '<=';
MINUS               : '-';
NOTEQUALS           : '!=';
PARENO              : '(';
PARENC              : ')';
PERCENT             : '%';
PLUS                : '+';
SLASH               : '/';
SEMICOLON           : ';';

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

BACKTICK:                       '`' {this.increaseTemplateDepth();} -> pushMode(TEMPLATE);

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

mode TEMPLATE;

BACKTICKINSIDE:                 '`' {this.decreaseTemplateDepth();} -> type(BACKTICK), popMode;
TEMPLATESTRINGSTARTEXPRESSION:  '${' -> pushMode(DEFAULT_MODE);
TEMPLATESTRINGATOM:             ~[`];

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

