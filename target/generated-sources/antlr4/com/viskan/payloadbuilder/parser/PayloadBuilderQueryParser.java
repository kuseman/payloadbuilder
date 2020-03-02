// Generated from PayloadBuilderQuery.g4 by ANTLR 4.7.1

//CSOFF
//@formatter:off
package com.viskan.payloadbuilder.parser;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class PayloadBuilderQueryParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, OPAREN=4, CPAREN=5, AND=6, ARRAY=7, AS=8, ASC=9, 
		COMMA=10, DESC=11, DOT=12, EXISTS=13, FALSE=14, FIRST=15, FROM=16, IN=17, 
		INNER=18, IS=19, JOIN=20, LAST=21, LEFT=22, NOT=23, NULL=24, NULLS=25, 
		OBJECT=26, ON=27, OR=28, ORDERBY=29, SELECT=30, TRUE=31, WHERE=32, ASTERISK=33, 
		EQUALS=34, EXCLAMATION=35, GREATERTHAN=36, GREATERTHANEQUAL=37, LESSTHAN=38, 
		LESSTHANEQUAL=39, MINUS=40, NOTEQUALS=41, PERCENT=42, PLUS=43, SLASH=44, 
		NUMBER=45, DECIMAL=46, STRING=47, IDENTIFIER=48, QUOTED_IDENTIFIER=49, 
		Comment=50, Space=51;
	public static final int
		RULE_query = 0, RULE_selectItem = 1, RULE_nestedSelectItem = 2, RULE_sortItem = 3, 
		RULE_expression = 4, RULE_primary = 5, RULE_functionCall = 6, RULE_literal = 7, 
		RULE_compareOperator = 8, RULE_qname = 9, RULE_identifier = 10, RULE_numericLiteral = 11, 
		RULE_decimalLiteral = 12, RULE_stringLiteral = 13, RULE_booleanLiteral = 14;
	public static final String[] ruleNames = {
		"query", "selectItem", "nestedSelectItem", "sortItem", "expression", "primary", 
		"functionCall", "literal", "compareOperator", "qname", "identifier", "numericLiteral", 
		"decimalLiteral", "stringLiteral", "booleanLiteral"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'->'", "'['", "']'", "'('", "')'", "'and'", "'array'", "'as'", 
		"'asc'", "','", "'desc'", "'.'", "'exists'", "'false'", "'first'", "'from'", 
		"'in'", "'inner'", "'is'", "'join'", "'last'", "'left'", "'not'", "'null'", 
		"'nulls'", "'object'", "'on'", "'or'", "'order by'", "'select'", "'true'", 
		"'where'", "'*'", "'='", "'!'", "'>'", "'>='", "'<'", "'<='", "'-'", "'!='", 
		"'%'", "'+'", "'/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, "OPAREN", "CPAREN", "AND", "ARRAY", "AS", "ASC", 
		"COMMA", "DESC", "DOT", "EXISTS", "FALSE", "FIRST", "FROM", "IN", "INNER", 
		"IS", "JOIN", "LAST", "LEFT", "NOT", "NULL", "NULLS", "OBJECT", "ON", 
		"OR", "ORDERBY", "SELECT", "TRUE", "WHERE", "ASTERISK", "EQUALS", "EXCLAMATION", 
		"GREATERTHAN", "GREATERTHANEQUAL", "LESSTHAN", "LESSTHANEQUAL", "MINUS", 
		"NOTEQUALS", "PERCENT", "PLUS", "SLASH", "NUMBER", "DECIMAL", "STRING", 
		"IDENTIFIER", "QUOTED_IDENTIFIER", "Comment", "Space"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "PayloadBuilderQuery.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public PayloadBuilderQueryParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class QueryContext extends ParserRuleContext {
		public QnameContext from;
		public IdentifierContext alias;
		public ExpressionContext where;
		public TerminalNode SELECT() { return getToken(PayloadBuilderQueryParser.SELECT, 0); }
		public List<SelectItemContext> selectItem() {
			return getRuleContexts(SelectItemContext.class);
		}
		public SelectItemContext selectItem(int i) {
			return getRuleContext(SelectItemContext.class,i);
		}
		public TerminalNode FROM() { return getToken(PayloadBuilderQueryParser.FROM, 0); }
		public TerminalNode EOF() { return getToken(PayloadBuilderQueryParser.EOF, 0); }
		public QnameContext qname() {
			return getRuleContext(QnameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(PayloadBuilderQueryParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PayloadBuilderQueryParser.COMMA, i);
		}
		public TerminalNode WHERE() { return getToken(PayloadBuilderQueryParser.WHERE, 0); }
		public TerminalNode ORDERBY() { return getToken(PayloadBuilderQueryParser.ORDERBY, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(30);
			match(SELECT);
			setState(31);
			selectItem();
			setState(36);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(32);
				match(COMMA);
				setState(33);
				selectItem();
				}
				}
				setState(38);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(39);
			match(FROM);
			setState(40);
			((QueryContext)_localctx).from = qname();
			setState(41);
			((QueryContext)_localctx).alias = identifier();
			setState(44);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(42);
				match(WHERE);
				setState(43);
				((QueryContext)_localctx).where = expression(0);
				}
			}

			setState(55);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDERBY) {
				{
				setState(46);
				match(ORDERBY);
				setState(47);
				sortItem();
				setState(52);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(48);
					match(COMMA);
					setState(49);
					sortItem();
					}
					}
					setState(54);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(57);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectItemContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public NestedSelectItemContext nestedSelectItem() {
			return getRuleContext(NestedSelectItemContext.class,0);
		}
		public TerminalNode OBJECT() { return getToken(PayloadBuilderQueryParser.OBJECT, 0); }
		public TerminalNode ARRAY() { return getToken(PayloadBuilderQueryParser.ARRAY, 0); }
		public TerminalNode AS() { return getToken(PayloadBuilderQueryParser.AS, 0); }
		public SelectItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectItem; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitSelectItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectItemContext selectItem() throws RecognitionException {
		SelectItemContext _localctx = new SelectItemContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_selectItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(62);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ARRAY:
			case OBJECT:
				{
				{
				setState(59);
				_la = _input.LA(1);
				if ( !(_la==ARRAY || _la==OBJECT) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(60);
				nestedSelectItem();
				}
				}
				break;
			case OPAREN:
			case FALSE:
			case NOT:
			case NULL:
			case TRUE:
			case MINUS:
			case PLUS:
			case NUMBER:
			case DECIMAL:
			case STRING:
			case IDENTIFIER:
			case QUOTED_IDENTIFIER:
				{
				setState(61);
				expression(0);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(68);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AS) | (1L << IDENTIFIER) | (1L << QUOTED_IDENTIFIER))) != 0)) {
				{
				setState(65);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(64);
					match(AS);
					}
				}

				setState(67);
				identifier();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NestedSelectItemContext extends ParserRuleContext {
		public QnameContext from;
		public ExpressionContext where;
		public TerminalNode OPAREN() { return getToken(PayloadBuilderQueryParser.OPAREN, 0); }
		public List<SelectItemContext> selectItem() {
			return getRuleContexts(SelectItemContext.class);
		}
		public SelectItemContext selectItem(int i) {
			return getRuleContext(SelectItemContext.class,i);
		}
		public TerminalNode CPAREN() { return getToken(PayloadBuilderQueryParser.CPAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(PayloadBuilderQueryParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PayloadBuilderQueryParser.COMMA, i);
		}
		public TerminalNode FROM() { return getToken(PayloadBuilderQueryParser.FROM, 0); }
		public TerminalNode WHERE() { return getToken(PayloadBuilderQueryParser.WHERE, 0); }
		public QnameContext qname() {
			return getRuleContext(QnameContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public NestedSelectItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nestedSelectItem; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitNestedSelectItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NestedSelectItemContext nestedSelectItem() throws RecognitionException {
		NestedSelectItemContext _localctx = new NestedSelectItemContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_nestedSelectItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(70);
			match(OPAREN);
			setState(71);
			selectItem();
			setState(76);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(72);
				match(COMMA);
				setState(73);
				selectItem();
				}
				}
				setState(78);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(81);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FROM) {
				{
				setState(79);
				match(FROM);
				setState(80);
				((NestedSelectItemContext)_localctx).from = qname();
				}
			}

			setState(85);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(83);
				match(WHERE);
				setState(84);
				((NestedSelectItemContext)_localctx).where = expression(0);
				}
			}

			setState(87);
			match(CPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SortItemContext extends ParserRuleContext {
		public Token order;
		public Token nullOrder;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(PayloadBuilderQueryParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(PayloadBuilderQueryParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(PayloadBuilderQueryParser.DESC, 0); }
		public TerminalNode FIRST() { return getToken(PayloadBuilderQueryParser.FIRST, 0); }
		public TerminalNode LAST() { return getToken(PayloadBuilderQueryParser.LAST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(89);
			expression(0);
			setState(91);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(90);
				((SortItemContext)_localctx).order = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).order = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(95);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(93);
				match(NULLS);
				setState(94);
				((SortItemContext)_localctx).nullOrder = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrder = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
	 
		public ExpressionContext() { }
		public void copyFrom(ExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class PrimaryExpressionContext extends ExpressionContext {
		public PrimaryContext primary() {
			return getRuleContext(PrimaryContext.class,0);
		}
		public PrimaryExpressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitPrimaryExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LogicalNotContext extends ExpressionContext {
		public TerminalNode NOT() { return getToken(PayloadBuilderQueryParser.NOT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public LogicalNotContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InExpressionContext extends ExpressionContext {
		public ExpressionContext left;
		public TerminalNode IN() { return getToken(PayloadBuilderQueryParser.IN, 0); }
		public TerminalNode OPAREN() { return getToken(PayloadBuilderQueryParser.OPAREN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode CPAREN() { return getToken(PayloadBuilderQueryParser.CPAREN, 0); }
		public TerminalNode NOT() { return getToken(PayloadBuilderQueryParser.NOT, 0); }
		public List<TerminalNode> COMMA() { return getTokens(PayloadBuilderQueryParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PayloadBuilderQueryParser.COMMA, i);
		}
		public InExpressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitInExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ComparisonExpressionContext extends ExpressionContext {
		public ExpressionContext left;
		public Token op;
		public ExpressionContext right;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode EQUALS() { return getToken(PayloadBuilderQueryParser.EQUALS, 0); }
		public TerminalNode NOTEQUALS() { return getToken(PayloadBuilderQueryParser.NOTEQUALS, 0); }
		public TerminalNode LESSTHAN() { return getToken(PayloadBuilderQueryParser.LESSTHAN, 0); }
		public TerminalNode LESSTHANEQUAL() { return getToken(PayloadBuilderQueryParser.LESSTHANEQUAL, 0); }
		public TerminalNode GREATERTHAN() { return getToken(PayloadBuilderQueryParser.GREATERTHAN, 0); }
		public TerminalNode GREATERTHANEQUAL() { return getToken(PayloadBuilderQueryParser.GREATERTHANEQUAL, 0); }
		public ComparisonExpressionContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitComparisonExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticBinaryContext extends ExpressionContext {
		public ExpressionContext left;
		public Token op;
		public ExpressionContext right;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(PayloadBuilderQueryParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(PayloadBuilderQueryParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(PayloadBuilderQueryParser.PERCENT, 0); }
		public TerminalNode PLUS() { return getToken(PayloadBuilderQueryParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(PayloadBuilderQueryParser.MINUS, 0); }
		public ArithmeticBinaryContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticUnaryContext extends ExpressionContext {
		public Token op;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(PayloadBuilderQueryParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(PayloadBuilderQueryParser.PLUS, 0); }
		public ArithmeticUnaryContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NullPredicateContext extends ExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode IS() { return getToken(PayloadBuilderQueryParser.IS, 0); }
		public TerminalNode NULL() { return getToken(PayloadBuilderQueryParser.NULL, 0); }
		public TerminalNode NOT() { return getToken(PayloadBuilderQueryParser.NOT, 0); }
		public NullPredicateContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitNullPredicate(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LogicalBinaryContext extends ExpressionContext {
		public ExpressionContext left;
		public Token op;
		public ExpressionContext right;
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(PayloadBuilderQueryParser.AND, 0); }
		public TerminalNode OR() { return getToken(PayloadBuilderQueryParser.OR, 0); }
		public LogicalBinaryContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitLogicalBinary(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		return expression(0);
	}

	private ExpressionContext expression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExpressionContext _localctx = new ExpressionContext(_ctx, _parentState);
		ExpressionContext _prevctx = _localctx;
		int _startState = 8;
		enterRecursionRule(_localctx, 8, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(103);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case OPAREN:
			case FALSE:
			case NULL:
			case TRUE:
			case NUMBER:
			case DECIMAL:
			case STRING:
			case IDENTIFIER:
			case QUOTED_IDENTIFIER:
				{
				_localctx = new PrimaryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(98);
				primary(0);
				}
				break;
			case MINUS:
			case PLUS:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(99);
				((ArithmeticUnaryContext)_localctx).op = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==MINUS || _la==PLUS) ) {
					((ArithmeticUnaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(100);
				expression(7);
				}
				break;
			case NOT:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(101);
				match(NOT);
				setState(102);
				expression(2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(138);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(136);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,16,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(105);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(106);
						((ArithmeticBinaryContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ASTERISK) | (1L << MINUS) | (1L << PERCENT) | (1L << PLUS) | (1L << SLASH))) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(107);
						((ArithmeticBinaryContext)_localctx).right = expression(7);
						}
						break;
					case 2:
						{
						_localctx = new ComparisonExpressionContext(new ExpressionContext(_parentctx, _parentState));
						((ComparisonExpressionContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(108);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(109);
						((ComparisonExpressionContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQUALS) | (1L << GREATERTHAN) | (1L << GREATERTHANEQUAL) | (1L << LESSTHAN) | (1L << LESSTHANEQUAL) | (1L << NOTEQUALS))) != 0)) ) {
							((ComparisonExpressionContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(110);
						((ComparisonExpressionContext)_localctx).right = expression(6);
						}
						break;
					case 3:
						{
						_localctx = new LogicalBinaryContext(new ExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(111);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(112);
						((LogicalBinaryContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==AND || _la==OR) ) {
							((LogicalBinaryContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(113);
						((LogicalBinaryContext)_localctx).right = expression(2);
						}
						break;
					case 4:
						{
						_localctx = new InExpressionContext(new ExpressionContext(_parentctx, _parentState));
						((InExpressionContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(114);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(116);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==NOT) {
							{
							setState(115);
							match(NOT);
							}
						}

						setState(118);
						match(IN);
						setState(119);
						match(OPAREN);
						setState(120);
						expression(0);
						setState(125);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
							{
							setState(121);
							match(COMMA);
							setState(122);
							expression(0);
							}
							}
							setState(127);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(128);
						match(CPAREN);
						}
						break;
					case 5:
						{
						_localctx = new NullPredicateContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(130);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(131);
						match(IS);
						setState(133);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==NOT) {
							{
							setState(132);
							match(NOT);
							}
						}

						setState(135);
						match(NULL);
						}
						break;
					}
					} 
				}
				setState(140);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PrimaryContext extends ParserRuleContext {
		public PrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primary; }
	 
		public PrimaryContext() { }
		public void copyFrom(PrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class DereferenceContext extends PrimaryContext {
		public PrimaryContext left;
		public TerminalNode DOT() { return getToken(PayloadBuilderQueryParser.DOT, 0); }
		public PrimaryContext primary() {
			return getRuleContext(PrimaryContext.class,0);
		}
		public FunctionCallContext functionCall() {
			return getRuleContext(FunctionCallContext.class,0);
		}
		public QnameContext qname() {
			return getRuleContext(QnameContext.class,0);
		}
		public DereferenceContext(PrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LambdaExpressionContext extends PrimaryContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public LambdaExpressionContext(PrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitLambdaExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ColumnReferenceContext extends PrimaryContext {
		public QnameContext qname() {
			return getRuleContext(QnameContext.class,0);
		}
		public ColumnReferenceContext(PrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubscriptContext extends PrimaryContext {
		public PrimaryContext value;
		public ExpressionContext index;
		public PrimaryContext primary() {
			return getRuleContext(PrimaryContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitSubscript(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NestedExpressionContext extends PrimaryContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public NestedExpressionContext(PrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitNestedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FunctionCallExpressionContext extends PrimaryContext {
		public FunctionCallContext functionCall() {
			return getRuleContext(FunctionCallContext.class,0);
		}
		public FunctionCallExpressionContext(PrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitFunctionCallExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LiteralExpressionContext extends PrimaryContext {
		public LiteralContext literal() {
			return getRuleContext(LiteralContext.class,0);
		}
		public LiteralExpressionContext(PrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitLiteralExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryContext primary() throws RecognitionException {
		return primary(0);
	}

	private PrimaryContext primary(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryContext _localctx = new PrimaryContext(_ctx, _parentState);
		PrimaryContext _prevctx = _localctx;
		int _startState = 10;
		enterRecursionRule(_localctx, 10, RULE_primary, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(165);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
			case 1:
				{
				_localctx = new LiteralExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(142);
				literal();
				}
				break;
			case 2:
				{
				_localctx = new FunctionCallExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(143);
				functionCall();
				}
				break;
			case 3:
				{
				_localctx = new LambdaExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(144);
				identifier();
				setState(145);
				match(T__0);
				setState(146);
				expression(0);
				}
				break;
			case 4:
				{
				_localctx = new LambdaExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(148);
				match(OPAREN);
				setState(149);
				identifier();
				setState(152); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(150);
					match(COMMA);
					setState(151);
					identifier();
					}
					}
					setState(154); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==COMMA );
				setState(156);
				match(CPAREN);
				setState(157);
				match(T__0);
				setState(158);
				expression(0);
				}
				break;
			case 5:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(160);
				qname();
				}
				break;
			case 6:
				{
				_localctx = new NestedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(161);
				match(OPAREN);
				setState(162);
				expression(0);
				setState(163);
				match(CPAREN);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(180);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,22,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(178);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primary);
						setState(167);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(168);
						match(T__1);
						setState(169);
						((SubscriptContext)_localctx).index = expression(0);
						setState(170);
						match(T__2);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primary);
						setState(172);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(173);
						match(DOT);
						setState(176);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
						case 1:
							{
							setState(174);
							functionCall();
							}
							break;
						case 2:
							{
							setState(175);
							qname();
							}
							break;
						}
						}
						break;
					}
					} 
				}
				setState(182);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,22,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class FunctionCallContext extends ParserRuleContext {
		public QnameContext qname() {
			return getRuleContext(QnameContext.class,0);
		}
		public TerminalNode OPAREN() { return getToken(PayloadBuilderQueryParser.OPAREN, 0); }
		public TerminalNode CPAREN() { return getToken(PayloadBuilderQueryParser.CPAREN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(PayloadBuilderQueryParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(PayloadBuilderQueryParser.COMMA, i);
		}
		public FunctionCallContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionCall; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionCallContext functionCall() throws RecognitionException {
		FunctionCallContext _localctx = new FunctionCallContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_functionCall);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(183);
			qname();
			setState(184);
			match(OPAREN);
			setState(193);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OPAREN) | (1L << FALSE) | (1L << NOT) | (1L << NULL) | (1L << TRUE) | (1L << MINUS) | (1L << PLUS) | (1L << NUMBER) | (1L << DECIMAL) | (1L << STRING) | (1L << IDENTIFIER) | (1L << QUOTED_IDENTIFIER))) != 0)) {
				{
				setState(185);
				expression(0);
				setState(190);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(186);
					match(COMMA);
					setState(187);
					expression(0);
					}
					}
					setState(192);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(195);
			match(CPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LiteralContext extends ParserRuleContext {
		public TerminalNode NULL() { return getToken(PayloadBuilderQueryParser.NULL, 0); }
		public BooleanLiteralContext booleanLiteral() {
			return getRuleContext(BooleanLiteralContext.class,0);
		}
		public NumericLiteralContext numericLiteral() {
			return getRuleContext(NumericLiteralContext.class,0);
		}
		public DecimalLiteralContext decimalLiteral() {
			return getRuleContext(DecimalLiteralContext.class,0);
		}
		public StringLiteralContext stringLiteral() {
			return getRuleContext(StringLiteralContext.class,0);
		}
		public LiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LiteralContext literal() throws RecognitionException {
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_literal);
		try {
			setState(202);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NULL:
				enterOuterAlt(_localctx, 1);
				{
				setState(197);
				match(NULL);
				}
				break;
			case FALSE:
			case TRUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(198);
				booleanLiteral();
				}
				break;
			case NUMBER:
				enterOuterAlt(_localctx, 3);
				{
				setState(199);
				numericLiteral();
				}
				break;
			case DECIMAL:
				enterOuterAlt(_localctx, 4);
				{
				setState(200);
				decimalLiteral();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 5);
				{
				setState(201);
				stringLiteral();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CompareOperatorContext extends ParserRuleContext {
		public TerminalNode EQUALS() { return getToken(PayloadBuilderQueryParser.EQUALS, 0); }
		public TerminalNode NOTEQUALS() { return getToken(PayloadBuilderQueryParser.NOTEQUALS, 0); }
		public TerminalNode LESSTHAN() { return getToken(PayloadBuilderQueryParser.LESSTHAN, 0); }
		public TerminalNode LESSTHANEQUAL() { return getToken(PayloadBuilderQueryParser.LESSTHANEQUAL, 0); }
		public TerminalNode GREATERTHAN() { return getToken(PayloadBuilderQueryParser.GREATERTHAN, 0); }
		public TerminalNode GREATERTHANEQUAL() { return getToken(PayloadBuilderQueryParser.GREATERTHANEQUAL, 0); }
		public CompareOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_compareOperator; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitCompareOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CompareOperatorContext compareOperator() throws RecognitionException {
		CompareOperatorContext _localctx = new CompareOperatorContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_compareOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(204);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << EQUALS) | (1L << GREATERTHAN) | (1L << GREATERTHANEQUAL) | (1L << LESSTHAN) | (1L << LESSTHANEQUAL) | (1L << NOTEQUALS))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QnameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(PayloadBuilderQueryParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(PayloadBuilderQueryParser.DOT, i);
		}
		public QnameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qname; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitQname(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QnameContext qname() throws RecognitionException {
		QnameContext _localctx = new QnameContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_qname);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(206);
			identifier();
			setState(211);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,26,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(207);
					match(DOT);
					setState(208);
					identifier();
					}
					} 
				}
				setState(213);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,26,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(PayloadBuilderQueryParser.IDENTIFIER, 0); }
		public TerminalNode QUOTED_IDENTIFIER() { return getToken(PayloadBuilderQueryParser.QUOTED_IDENTIFIER, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_identifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(214);
			_la = _input.LA(1);
			if ( !(_la==IDENTIFIER || _la==QUOTED_IDENTIFIER) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NumericLiteralContext extends ParserRuleContext {
		public TerminalNode NUMBER() { return getToken(PayloadBuilderQueryParser.NUMBER, 0); }
		public NumericLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_numericLiteral; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumericLiteralContext numericLiteral() throws RecognitionException {
		NumericLiteralContext _localctx = new NumericLiteralContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_numericLiteral);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(216);
			match(NUMBER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DecimalLiteralContext extends ParserRuleContext {
		public TerminalNode DECIMAL() { return getToken(PayloadBuilderQueryParser.DECIMAL, 0); }
		public DecimalLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_decimalLiteral; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DecimalLiteralContext decimalLiteral() throws RecognitionException {
		DecimalLiteralContext _localctx = new DecimalLiteralContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_decimalLiteral);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(218);
			match(DECIMAL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StringLiteralContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(PayloadBuilderQueryParser.STRING, 0); }
		public StringLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stringLiteral; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StringLiteralContext stringLiteral() throws RecognitionException {
		StringLiteralContext _localctx = new StringLiteralContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_stringLiteral);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(220);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanLiteralContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(PayloadBuilderQueryParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(PayloadBuilderQueryParser.FALSE, 0); }
		public BooleanLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanLiteral; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanLiteralContext booleanLiteral() throws RecognitionException {
		BooleanLiteralContext _localctx = new BooleanLiteralContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_booleanLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(222);
			_la = _input.LA(1);
			if ( !(_la==FALSE || _la==TRUE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 4:
			return expression_sempred((ExpressionContext)_localctx, predIndex);
		case 5:
			return primary_sempred((PrimaryContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expression_sempred(ExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 6);
		case 1:
			return precpred(_ctx, 5);
		case 2:
			return precpred(_ctx, 1);
		case 3:
			return precpred(_ctx, 4);
		case 4:
			return precpred(_ctx, 3);
		}
		return true;
	}
	private boolean primary_sempred(PrimaryContext _localctx, int predIndex) {
		switch (predIndex) {
		case 5:
			return precpred(_ctx, 4);
		case 6:
			return precpred(_ctx, 2);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\65\u00e3\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\3\2\3\2\3\2\3\2\7"+
		"\2%\n\2\f\2\16\2(\13\2\3\2\3\2\3\2\3\2\3\2\5\2/\n\2\3\2\3\2\3\2\3\2\7"+
		"\2\65\n\2\f\2\16\28\13\2\5\2:\n\2\3\2\3\2\3\3\3\3\3\3\5\3A\n\3\3\3\5\3"+
		"D\n\3\3\3\5\3G\n\3\3\4\3\4\3\4\3\4\7\4M\n\4\f\4\16\4P\13\4\3\4\3\4\5\4"+
		"T\n\4\3\4\3\4\5\4X\n\4\3\4\3\4\3\5\3\5\5\5^\n\5\3\5\3\5\5\5b\n\5\3\6\3"+
		"\6\3\6\3\6\3\6\3\6\5\6j\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\5\6w\n\6\3\6\3\6\3\6\3\6\3\6\7\6~\n\6\f\6\16\6\u0081\13\6\3\6\3\6\3"+
		"\6\3\6\3\6\5\6\u0088\n\6\3\6\7\6\u008b\n\6\f\6\16\6\u008e\13\6\3\7\3\7"+
		"\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\6\7\u009b\n\7\r\7\16\7\u009c\3\7"+
		"\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u00a8\n\7\3\7\3\7\3\7\3\7\3\7\3\7"+
		"\3\7\3\7\3\7\5\7\u00b3\n\7\7\7\u00b5\n\7\f\7\16\7\u00b8\13\7\3\b\3\b\3"+
		"\b\3\b\3\b\7\b\u00bf\n\b\f\b\16\b\u00c2\13\b\5\b\u00c4\n\b\3\b\3\b\3\t"+
		"\3\t\3\t\3\t\3\t\5\t\u00cd\n\t\3\n\3\n\3\13\3\13\3\13\7\13\u00d4\n\13"+
		"\f\13\16\13\u00d7\13\13\3\f\3\f\3\r\3\r\3\16\3\16\3\17\3\17\3\20\3\20"+
		"\3\20\2\4\n\f\21\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36\2\13\4\2\t\t\34"+
		"\34\4\2\13\13\r\r\4\2\21\21\27\27\4\2**--\5\2##**,.\5\2$$&)++\4\2\b\b"+
		"\36\36\3\2\62\63\4\2\20\20!!\2\u00f9\2 \3\2\2\2\4@\3\2\2\2\6H\3\2\2\2"+
		"\b[\3\2\2\2\ni\3\2\2\2\f\u00a7\3\2\2\2\16\u00b9\3\2\2\2\20\u00cc\3\2\2"+
		"\2\22\u00ce\3\2\2\2\24\u00d0\3\2\2\2\26\u00d8\3\2\2\2\30\u00da\3\2\2\2"+
		"\32\u00dc\3\2\2\2\34\u00de\3\2\2\2\36\u00e0\3\2\2\2 !\7 \2\2!&\5\4\3\2"+
		"\"#\7\f\2\2#%\5\4\3\2$\"\3\2\2\2%(\3\2\2\2&$\3\2\2\2&\'\3\2\2\2\')\3\2"+
		"\2\2(&\3\2\2\2)*\7\22\2\2*+\5\24\13\2+.\5\26\f\2,-\7\"\2\2-/\5\n\6\2."+
		",\3\2\2\2./\3\2\2\2/9\3\2\2\2\60\61\7\37\2\2\61\66\5\b\5\2\62\63\7\f\2"+
		"\2\63\65\5\b\5\2\64\62\3\2\2\2\658\3\2\2\2\66\64\3\2\2\2\66\67\3\2\2\2"+
		"\67:\3\2\2\28\66\3\2\2\29\60\3\2\2\29:\3\2\2\2:;\3\2\2\2;<\7\2\2\3<\3"+
		"\3\2\2\2=>\t\2\2\2>A\5\6\4\2?A\5\n\6\2@=\3\2\2\2@?\3\2\2\2AF\3\2\2\2B"+
		"D\7\n\2\2CB\3\2\2\2CD\3\2\2\2DE\3\2\2\2EG\5\26\f\2FC\3\2\2\2FG\3\2\2\2"+
		"G\5\3\2\2\2HI\7\6\2\2IN\5\4\3\2JK\7\f\2\2KM\5\4\3\2LJ\3\2\2\2MP\3\2\2"+
		"\2NL\3\2\2\2NO\3\2\2\2OS\3\2\2\2PN\3\2\2\2QR\7\22\2\2RT\5\24\13\2SQ\3"+
		"\2\2\2ST\3\2\2\2TW\3\2\2\2UV\7\"\2\2VX\5\n\6\2WU\3\2\2\2WX\3\2\2\2XY\3"+
		"\2\2\2YZ\7\7\2\2Z\7\3\2\2\2[]\5\n\6\2\\^\t\3\2\2]\\\3\2\2\2]^\3\2\2\2"+
		"^a\3\2\2\2_`\7\33\2\2`b\t\4\2\2a_\3\2\2\2ab\3\2\2\2b\t\3\2\2\2cd\b\6\1"+
		"\2dj\5\f\7\2ef\t\5\2\2fj\5\n\6\tgh\7\31\2\2hj\5\n\6\4ic\3\2\2\2ie\3\2"+
		"\2\2ig\3\2\2\2j\u008c\3\2\2\2kl\f\b\2\2lm\t\6\2\2m\u008b\5\n\6\tno\f\7"+
		"\2\2op\t\7\2\2p\u008b\5\n\6\bqr\f\3\2\2rs\t\b\2\2s\u008b\5\n\6\4tv\f\6"+
		"\2\2uw\7\31\2\2vu\3\2\2\2vw\3\2\2\2wx\3\2\2\2xy\7\23\2\2yz\7\6\2\2z\177"+
		"\5\n\6\2{|\7\f\2\2|~\5\n\6\2}{\3\2\2\2~\u0081\3\2\2\2\177}\3\2\2\2\177"+
		"\u0080\3\2\2\2\u0080\u0082\3\2\2\2\u0081\177\3\2\2\2\u0082\u0083\7\7\2"+
		"\2\u0083\u008b\3\2\2\2\u0084\u0085\f\5\2\2\u0085\u0087\7\25\2\2\u0086"+
		"\u0088\7\31\2\2\u0087\u0086\3\2\2\2\u0087\u0088\3\2\2\2\u0088\u0089\3"+
		"\2\2\2\u0089\u008b\7\32\2\2\u008ak\3\2\2\2\u008an\3\2\2\2\u008aq\3\2\2"+
		"\2\u008at\3\2\2\2\u008a\u0084\3\2\2\2\u008b\u008e\3\2\2\2\u008c\u008a"+
		"\3\2\2\2\u008c\u008d\3\2\2\2\u008d\13\3\2\2\2\u008e\u008c\3\2\2\2\u008f"+
		"\u0090\b\7\1\2\u0090\u00a8\5\20\t\2\u0091\u00a8\5\16\b\2\u0092\u0093\5"+
		"\26\f\2\u0093\u0094\7\3\2\2\u0094\u0095\5\n\6\2\u0095\u00a8\3\2\2\2\u0096"+
		"\u0097\7\6\2\2\u0097\u009a\5\26\f\2\u0098\u0099\7\f\2\2\u0099\u009b\5"+
		"\26\f\2\u009a\u0098\3\2\2\2\u009b\u009c\3\2\2\2\u009c\u009a\3\2\2\2\u009c"+
		"\u009d\3\2\2\2\u009d\u009e\3\2\2\2\u009e\u009f\7\7\2\2\u009f\u00a0\7\3"+
		"\2\2\u00a0\u00a1\5\n\6\2\u00a1\u00a8\3\2\2\2\u00a2\u00a8\5\24\13\2\u00a3"+
		"\u00a4\7\6\2\2\u00a4\u00a5\5\n\6\2\u00a5\u00a6\7\7\2\2\u00a6\u00a8\3\2"+
		"\2\2\u00a7\u008f\3\2\2\2\u00a7\u0091\3\2\2\2\u00a7\u0092\3\2\2\2\u00a7"+
		"\u0096\3\2\2\2\u00a7\u00a2\3\2\2\2\u00a7\u00a3\3\2\2\2\u00a8\u00b6\3\2"+
		"\2\2\u00a9\u00aa\f\6\2\2\u00aa\u00ab\7\4\2\2\u00ab\u00ac\5\n\6\2\u00ac"+
		"\u00ad\7\5\2\2\u00ad\u00b5\3\2\2\2\u00ae\u00af\f\4\2\2\u00af\u00b2\7\16"+
		"\2\2\u00b0\u00b3\5\16\b\2\u00b1\u00b3\5\24\13\2\u00b2\u00b0\3\2\2\2\u00b2"+
		"\u00b1\3\2\2\2\u00b3\u00b5\3\2\2\2\u00b4\u00a9\3\2\2\2\u00b4\u00ae\3\2"+
		"\2\2\u00b5\u00b8\3\2\2\2\u00b6\u00b4\3\2\2\2\u00b6\u00b7\3\2\2\2\u00b7"+
		"\r\3\2\2\2\u00b8\u00b6\3\2\2\2\u00b9\u00ba\5\24\13\2\u00ba\u00c3\7\6\2"+
		"\2\u00bb\u00c0\5\n\6\2\u00bc\u00bd\7\f\2\2\u00bd\u00bf\5\n\6\2\u00be\u00bc"+
		"\3\2\2\2\u00bf\u00c2\3\2\2\2\u00c0\u00be\3\2\2\2\u00c0\u00c1\3\2\2\2\u00c1"+
		"\u00c4\3\2\2\2\u00c2\u00c0\3\2\2\2\u00c3\u00bb\3\2\2\2\u00c3\u00c4\3\2"+
		"\2\2\u00c4\u00c5\3\2\2\2\u00c5\u00c6\7\7\2\2\u00c6\17\3\2\2\2\u00c7\u00cd"+
		"\7\32\2\2\u00c8\u00cd\5\36\20\2\u00c9\u00cd\5\30\r\2\u00ca\u00cd\5\32"+
		"\16\2\u00cb\u00cd\5\34\17\2\u00cc\u00c7\3\2\2\2\u00cc\u00c8\3\2\2\2\u00cc"+
		"\u00c9\3\2\2\2\u00cc\u00ca\3\2\2\2\u00cc\u00cb\3\2\2\2\u00cd\21\3\2\2"+
		"\2\u00ce\u00cf\t\7\2\2\u00cf\23\3\2\2\2\u00d0\u00d5\5\26\f\2\u00d1\u00d2"+
		"\7\16\2\2\u00d2\u00d4\5\26\f\2\u00d3\u00d1\3\2\2\2\u00d4\u00d7\3\2\2\2"+
		"\u00d5\u00d3\3\2\2\2\u00d5\u00d6\3\2\2\2\u00d6\25\3\2\2\2\u00d7\u00d5"+
		"\3\2\2\2\u00d8\u00d9\t\t\2\2\u00d9\27\3\2\2\2\u00da\u00db\7/\2\2\u00db"+
		"\31\3\2\2\2\u00dc\u00dd\7\60\2\2\u00dd\33\3\2\2\2\u00de\u00df\7\61\2\2"+
		"\u00df\35\3\2\2\2\u00e0\u00e1\t\n\2\2\u00e1\37\3\2\2\2\35&.\669@CFNSW"+
		"]aiv\177\u0087\u008a\u008c\u009c\u00a7\u00b2\u00b4\u00b6\u00c0\u00c3\u00cc"+
		"\u00d5";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}