// Generated from com\viskan\payloadbuilder\parser\PayloadBuilderQuery.g4 by ANTLR 4.7.1
package com.viskan.payloadbuilder.parser;

//CSOFF
//@formatter:off

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
		RULE_query = 0, RULE_selectItem = 1, RULE_nestedSelectItem = 2, RULE_errorCapturingIdentifier = 3, 
		RULE_errorCapturingIdentifierExtra = 4, RULE_sortItem = 5, RULE_expression = 6, 
		RULE_primary = 7, RULE_functionCall = 8, RULE_literal = 9, RULE_compareOperator = 10, 
		RULE_qname = 11, RULE_identifier = 12, RULE_numericLiteral = 13, RULE_decimalLiteral = 14, 
		RULE_stringLiteral = 15, RULE_booleanLiteral = 16;
	public static final String[] ruleNames = {
		"query", "selectItem", "nestedSelectItem", "errorCapturingIdentifier", 
		"errorCapturingIdentifierExtra", "sortItem", "expression", "primary", 
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
			setState(34);
			match(SELECT);
			setState(35);
			selectItem();
			setState(40);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(36);
				match(COMMA);
				setState(37);
				selectItem();
				}
				}
				setState(42);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(43);
			match(FROM);
			setState(44);
			((QueryContext)_localctx).from = qname();
			setState(45);
			((QueryContext)_localctx).alias = identifier();
			setState(48);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(46);
				match(WHERE);
				setState(47);
				((QueryContext)_localctx).where = expression(0);
				}
			}

			setState(59);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDERBY) {
				{
				setState(50);
				match(ORDERBY);
				setState(51);
				sortItem();
				setState(56);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(52);
					match(COMMA);
					setState(53);
					sortItem();
					}
					}
					setState(58);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(61);
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
			setState(66);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ARRAY:
			case OBJECT:
				{
				{
				setState(63);
				_la = _input.LA(1);
				if ( !(_la==ARRAY || _la==OBJECT) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(64);
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
				setState(65);
				expression(0);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(72);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << AS) | (1L << IDENTIFIER) | (1L << QUOTED_IDENTIFIER))) != 0)) {
				{
				setState(69);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(68);
					match(AS);
					}
				}

				setState(71);
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
			setState(74);
			match(OPAREN);
			setState(75);
			selectItem();
			setState(80);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(76);
				match(COMMA);
				setState(77);
				selectItem();
				}
				}
				setState(82);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(85);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FROM) {
				{
				setState(83);
				match(FROM);
				setState(84);
				((NestedSelectItemContext)_localctx).from = qname();
				}
			}

			setState(89);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(87);
				match(WHERE);
				setState(88);
				((NestedSelectItemContext)_localctx).where = expression(0);
				}
			}

			setState(91);
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

	public static class ErrorCapturingIdentifierContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ErrorCapturingIdentifierExtraContext errorCapturingIdentifierExtra() {
			return getRuleContext(ErrorCapturingIdentifierExtraContext.class,0);
		}
		public ErrorCapturingIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingIdentifier; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitErrorCapturingIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErrorCapturingIdentifierContext errorCapturingIdentifier() throws RecognitionException {
		ErrorCapturingIdentifierContext _localctx = new ErrorCapturingIdentifierContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_errorCapturingIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(93);
			identifier();
			setState(94);
			errorCapturingIdentifierExtra();
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

	public static class ErrorCapturingIdentifierExtraContext extends ParserRuleContext {
		public ErrorCapturingIdentifierExtraContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorCapturingIdentifierExtra; }
	 
		public ErrorCapturingIdentifierExtraContext() { }
		public void copyFrom(ErrorCapturingIdentifierExtraContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ErrorIdentContext extends ErrorCapturingIdentifierExtraContext {
		public List<TerminalNode> MINUS() { return getTokens(PayloadBuilderQueryParser.MINUS); }
		public TerminalNode MINUS(int i) {
			return getToken(PayloadBuilderQueryParser.MINUS, i);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public ErrorIdentContext(ErrorCapturingIdentifierExtraContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitErrorIdent(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RealIdentContext extends ErrorCapturingIdentifierExtraContext {
		public RealIdentContext(ErrorCapturingIdentifierExtraContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PayloadBuilderQueryVisitor ) return ((PayloadBuilderQueryVisitor<? extends T>)visitor).visitRealIdent(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ErrorCapturingIdentifierExtraContext errorCapturingIdentifierExtra() throws RecognitionException {
		ErrorCapturingIdentifierExtraContext _localctx = new ErrorCapturingIdentifierExtraContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_errorCapturingIdentifierExtra);
		int _la;
		try {
			setState(103);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MINUS:
				_localctx = new ErrorIdentContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(98); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(96);
					match(MINUS);
					setState(97);
					identifier();
					}
					}
					setState(100); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==MINUS );
				}
				break;
			case EOF:
				_localctx = new RealIdentContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
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
		enterRule(_localctx, 10, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(105);
			expression(0);
			setState(107);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(106);
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

			setState(111);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(109);
				match(NULLS);
				setState(110);
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
		int _startState = 12;
		enterRecursionRule(_localctx, 12, RULE_expression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(119);
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

				setState(114);
				primary(0);
				}
				break;
			case MINUS:
			case PLUS:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(115);
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
				setState(116);
				expression(7);
				}
				break;
			case NOT:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(117);
				match(NOT);
				setState(118);
				expression(2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(154);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(152);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(121);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(122);
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
						setState(123);
						((ArithmeticBinaryContext)_localctx).right = expression(7);
						}
						break;
					case 2:
						{
						_localctx = new ComparisonExpressionContext(new ExpressionContext(_parentctx, _parentState));
						((ComparisonExpressionContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(124);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(125);
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
						setState(126);
						((ComparisonExpressionContext)_localctx).right = expression(6);
						}
						break;
					case 3:
						{
						_localctx = new LogicalBinaryContext(new ExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(127);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(128);
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
						setState(129);
						((LogicalBinaryContext)_localctx).right = expression(2);
						}
						break;
					case 4:
						{
						_localctx = new InExpressionContext(new ExpressionContext(_parentctx, _parentState));
						((InExpressionContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(130);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(132);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==NOT) {
							{
							setState(131);
							match(NOT);
							}
						}

						setState(134);
						match(IN);
						setState(135);
						match(OPAREN);
						setState(136);
						expression(0);
						setState(141);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==COMMA) {
							{
							{
							setState(137);
							match(COMMA);
							setState(138);
							expression(0);
							}
							}
							setState(143);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						setState(144);
						match(CPAREN);
						}
						break;
					case 5:
						{
						_localctx = new NullPredicateContext(new ExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expression);
						setState(146);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(147);
						match(IS);
						setState(149);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==NOT) {
							{
							setState(148);
							match(NOT);
							}
						}

						setState(151);
						match(NULL);
						}
						break;
					}
					} 
				}
				setState(156);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
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
		int _startState = 14;
		enterRecursionRule(_localctx, 14, RULE_primary, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(181);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,21,_ctx) ) {
			case 1:
				{
				_localctx = new LiteralExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(158);
				literal();
				}
				break;
			case 2:
				{
				_localctx = new FunctionCallExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(159);
				functionCall();
				}
				break;
			case 3:
				{
				_localctx = new LambdaExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(160);
				identifier();
				setState(161);
				match(T__0);
				setState(162);
				expression(0);
				}
				break;
			case 4:
				{
				_localctx = new LambdaExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(164);
				match(OPAREN);
				setState(165);
				identifier();
				setState(168); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(166);
					match(COMMA);
					setState(167);
					identifier();
					}
					}
					setState(170); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==COMMA );
				setState(172);
				match(CPAREN);
				setState(173);
				match(T__0);
				setState(174);
				expression(0);
				}
				break;
			case 5:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(176);
				qname();
				}
				break;
			case 6:
				{
				_localctx = new NestedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(177);
				match(OPAREN);
				setState(178);
				expression(0);
				setState(179);
				match(CPAREN);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(196);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(194);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primary);
						setState(183);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(184);
						match(T__1);
						setState(185);
						((SubscriptContext)_localctx).index = expression(0);
						setState(186);
						match(T__2);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primary);
						setState(188);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(189);
						match(DOT);
						setState(192);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,22,_ctx) ) {
						case 1:
							{
							setState(190);
							functionCall();
							}
							break;
						case 2:
							{
							setState(191);
							qname();
							}
							break;
						}
						}
						break;
					}
					} 
				}
				setState(198);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,24,_ctx);
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
		enterRule(_localctx, 16, RULE_functionCall);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(199);
			qname();
			setState(200);
			match(OPAREN);
			setState(209);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OPAREN) | (1L << FALSE) | (1L << NOT) | (1L << NULL) | (1L << TRUE) | (1L << MINUS) | (1L << PLUS) | (1L << NUMBER) | (1L << DECIMAL) | (1L << STRING) | (1L << IDENTIFIER) | (1L << QUOTED_IDENTIFIER))) != 0)) {
				{
				setState(201);
				expression(0);
				setState(206);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(202);
					match(COMMA);
					setState(203);
					expression(0);
					}
					}
					setState(208);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(211);
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
		enterRule(_localctx, 18, RULE_literal);
		try {
			setState(218);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NULL:
				enterOuterAlt(_localctx, 1);
				{
				setState(213);
				match(NULL);
				}
				break;
			case FALSE:
			case TRUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(214);
				booleanLiteral();
				}
				break;
			case NUMBER:
				enterOuterAlt(_localctx, 3);
				{
				setState(215);
				numericLiteral();
				}
				break;
			case DECIMAL:
				enterOuterAlt(_localctx, 4);
				{
				setState(216);
				decimalLiteral();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 5);
				{
				setState(217);
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
		enterRule(_localctx, 20, RULE_compareOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(220);
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
		enterRule(_localctx, 22, RULE_qname);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(222);
			identifier();
			setState(227);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,28,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(223);
					match(DOT);
					setState(224);
					identifier();
					}
					} 
				}
				setState(229);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,28,_ctx);
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
		enterRule(_localctx, 24, RULE_identifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(230);
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
		enterRule(_localctx, 26, RULE_numericLiteral);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(232);
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
		enterRule(_localctx, 28, RULE_decimalLiteral);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(234);
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
		enterRule(_localctx, 30, RULE_stringLiteral);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(236);
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
		enterRule(_localctx, 32, RULE_booleanLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(238);
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
		case 6:
			return expression_sempred((ExpressionContext)_localctx, predIndex);
		case 7:
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
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\65\u00f3\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\3\2\3\2\3\2\3\2\7\2)\n\2\f\2\16\2,\13\2\3\2\3\2\3\2\3\2\3\2\5\2\63\n"+
		"\2\3\2\3\2\3\2\3\2\7\29\n\2\f\2\16\2<\13\2\5\2>\n\2\3\2\3\2\3\3\3\3\3"+
		"\3\5\3E\n\3\3\3\5\3H\n\3\3\3\5\3K\n\3\3\4\3\4\3\4\3\4\7\4Q\n\4\f\4\16"+
		"\4T\13\4\3\4\3\4\5\4X\n\4\3\4\3\4\5\4\\\n\4\3\4\3\4\3\5\3\5\3\5\3\6\3"+
		"\6\6\6e\n\6\r\6\16\6f\3\6\5\6j\n\6\3\7\3\7\5\7n\n\7\3\7\3\7\5\7r\n\7\3"+
		"\b\3\b\3\b\3\b\3\b\3\b\5\bz\n\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3"+
		"\b\3\b\5\b\u0087\n\b\3\b\3\b\3\b\3\b\3\b\7\b\u008e\n\b\f\b\16\b\u0091"+
		"\13\b\3\b\3\b\3\b\3\b\3\b\5\b\u0098\n\b\3\b\7\b\u009b\n\b\f\b\16\b\u009e"+
		"\13\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\6\t\u00ab\n\t\r\t\16"+
		"\t\u00ac\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u00b8\n\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u00c3\n\t\7\t\u00c5\n\t\f\t\16\t\u00c8\13"+
		"\t\3\n\3\n\3\n\3\n\3\n\7\n\u00cf\n\n\f\n\16\n\u00d2\13\n\5\n\u00d4\n\n"+
		"\3\n\3\n\3\13\3\13\3\13\3\13\3\13\5\13\u00dd\n\13\3\f\3\f\3\r\3\r\3\r"+
		"\7\r\u00e4\n\r\f\r\16\r\u00e7\13\r\3\16\3\16\3\17\3\17\3\20\3\20\3\21"+
		"\3\21\3\22\3\22\3\22\2\4\16\20\23\2\4\6\b\n\f\16\20\22\24\26\30\32\34"+
		"\36 \"\2\13\4\2\t\t\34\34\4\2\13\13\r\r\4\2\21\21\27\27\4\2**--\5\2##"+
		"**,.\5\2$$&)++\4\2\b\b\36\36\3\2\62\63\4\2\20\20!!\2\u0109\2$\3\2\2\2"+
		"\4D\3\2\2\2\6L\3\2\2\2\b_\3\2\2\2\ni\3\2\2\2\fk\3\2\2\2\16y\3\2\2\2\20"+
		"\u00b7\3\2\2\2\22\u00c9\3\2\2\2\24\u00dc\3\2\2\2\26\u00de\3\2\2\2\30\u00e0"+
		"\3\2\2\2\32\u00e8\3\2\2\2\34\u00ea\3\2\2\2\36\u00ec\3\2\2\2 \u00ee\3\2"+
		"\2\2\"\u00f0\3\2\2\2$%\7 \2\2%*\5\4\3\2&\'\7\f\2\2\')\5\4\3\2(&\3\2\2"+
		"\2),\3\2\2\2*(\3\2\2\2*+\3\2\2\2+-\3\2\2\2,*\3\2\2\2-.\7\22\2\2./\5\30"+
		"\r\2/\62\5\32\16\2\60\61\7\"\2\2\61\63\5\16\b\2\62\60\3\2\2\2\62\63\3"+
		"\2\2\2\63=\3\2\2\2\64\65\7\37\2\2\65:\5\f\7\2\66\67\7\f\2\2\679\5\f\7"+
		"\28\66\3\2\2\29<\3\2\2\2:8\3\2\2\2:;\3\2\2\2;>\3\2\2\2<:\3\2\2\2=\64\3"+
		"\2\2\2=>\3\2\2\2>?\3\2\2\2?@\7\2\2\3@\3\3\2\2\2AB\t\2\2\2BE\5\6\4\2CE"+
		"\5\16\b\2DA\3\2\2\2DC\3\2\2\2EJ\3\2\2\2FH\7\n\2\2GF\3\2\2\2GH\3\2\2\2"+
		"HI\3\2\2\2IK\5\32\16\2JG\3\2\2\2JK\3\2\2\2K\5\3\2\2\2LM\7\6\2\2MR\5\4"+
		"\3\2NO\7\f\2\2OQ\5\4\3\2PN\3\2\2\2QT\3\2\2\2RP\3\2\2\2RS\3\2\2\2SW\3\2"+
		"\2\2TR\3\2\2\2UV\7\22\2\2VX\5\30\r\2WU\3\2\2\2WX\3\2\2\2X[\3\2\2\2YZ\7"+
		"\"\2\2Z\\\5\16\b\2[Y\3\2\2\2[\\\3\2\2\2\\]\3\2\2\2]^\7\7\2\2^\7\3\2\2"+
		"\2_`\5\32\16\2`a\5\n\6\2a\t\3\2\2\2bc\7*\2\2ce\5\32\16\2db\3\2\2\2ef\3"+
		"\2\2\2fd\3\2\2\2fg\3\2\2\2gj\3\2\2\2hj\3\2\2\2id\3\2\2\2ih\3\2\2\2j\13"+
		"\3\2\2\2km\5\16\b\2ln\t\3\2\2ml\3\2\2\2mn\3\2\2\2nq\3\2\2\2op\7\33\2\2"+
		"pr\t\4\2\2qo\3\2\2\2qr\3\2\2\2r\r\3\2\2\2st\b\b\1\2tz\5\20\t\2uv\t\5\2"+
		"\2vz\5\16\b\twx\7\31\2\2xz\5\16\b\4ys\3\2\2\2yu\3\2\2\2yw\3\2\2\2z\u009c"+
		"\3\2\2\2{|\f\b\2\2|}\t\6\2\2}\u009b\5\16\b\t~\177\f\7\2\2\177\u0080\t"+
		"\7\2\2\u0080\u009b\5\16\b\b\u0081\u0082\f\3\2\2\u0082\u0083\t\b\2\2\u0083"+
		"\u009b\5\16\b\4\u0084\u0086\f\6\2\2\u0085\u0087\7\31\2\2\u0086\u0085\3"+
		"\2\2\2\u0086\u0087\3\2\2\2\u0087\u0088\3\2\2\2\u0088\u0089\7\23\2\2\u0089"+
		"\u008a\7\6\2\2\u008a\u008f\5\16\b\2\u008b\u008c\7\f\2\2\u008c\u008e\5"+
		"\16\b\2\u008d\u008b\3\2\2\2\u008e\u0091\3\2\2\2\u008f\u008d\3\2\2\2\u008f"+
		"\u0090\3\2\2\2\u0090\u0092\3\2\2\2\u0091\u008f\3\2\2\2\u0092\u0093\7\7"+
		"\2\2\u0093\u009b\3\2\2\2\u0094\u0095\f\5\2\2\u0095\u0097\7\25\2\2\u0096"+
		"\u0098\7\31\2\2\u0097\u0096\3\2\2\2\u0097\u0098\3\2\2\2\u0098\u0099\3"+
		"\2\2\2\u0099\u009b\7\32\2\2\u009a{\3\2\2\2\u009a~\3\2\2\2\u009a\u0081"+
		"\3\2\2\2\u009a\u0084\3\2\2\2\u009a\u0094\3\2\2\2\u009b\u009e\3\2\2\2\u009c"+
		"\u009a\3\2\2\2\u009c\u009d\3\2\2\2\u009d\17\3\2\2\2\u009e\u009c\3\2\2"+
		"\2\u009f\u00a0\b\t\1\2\u00a0\u00b8\5\24\13\2\u00a1\u00b8\5\22\n\2\u00a2"+
		"\u00a3\5\32\16\2\u00a3\u00a4\7\3\2\2\u00a4\u00a5\5\16\b\2\u00a5\u00b8"+
		"\3\2\2\2\u00a6\u00a7\7\6\2\2\u00a7\u00aa\5\32\16\2\u00a8\u00a9\7\f\2\2"+
		"\u00a9\u00ab\5\32\16\2\u00aa\u00a8\3\2\2\2\u00ab\u00ac\3\2\2\2\u00ac\u00aa"+
		"\3\2\2\2\u00ac\u00ad\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ae\u00af\7\7\2\2\u00af"+
		"\u00b0\7\3\2\2\u00b0\u00b1\5\16\b\2\u00b1\u00b8\3\2\2\2\u00b2\u00b8\5"+
		"\30\r\2\u00b3\u00b4\7\6\2\2\u00b4\u00b5\5\16\b\2\u00b5\u00b6\7\7\2\2\u00b6"+
		"\u00b8\3\2\2\2\u00b7\u009f\3\2\2\2\u00b7\u00a1\3\2\2\2\u00b7\u00a2\3\2"+
		"\2\2\u00b7\u00a6\3\2\2\2\u00b7\u00b2\3\2\2\2\u00b7\u00b3\3\2\2\2\u00b8"+
		"\u00c6\3\2\2\2\u00b9\u00ba\f\6\2\2\u00ba\u00bb\7\4\2\2\u00bb\u00bc\5\16"+
		"\b\2\u00bc\u00bd\7\5\2\2\u00bd\u00c5\3\2\2\2\u00be\u00bf\f\4\2\2\u00bf"+
		"\u00c2\7\16\2\2\u00c0\u00c3\5\22\n\2\u00c1\u00c3\5\30\r\2\u00c2\u00c0"+
		"\3\2\2\2\u00c2\u00c1\3\2\2\2\u00c3\u00c5\3\2\2\2\u00c4\u00b9\3\2\2\2\u00c4"+
		"\u00be\3\2\2\2\u00c5\u00c8\3\2\2\2\u00c6\u00c4\3\2\2\2\u00c6\u00c7\3\2"+
		"\2\2\u00c7\21\3\2\2\2\u00c8\u00c6\3\2\2\2\u00c9\u00ca\5\30\r\2\u00ca\u00d3"+
		"\7\6\2\2\u00cb\u00d0\5\16\b\2\u00cc\u00cd\7\f\2\2\u00cd\u00cf\5\16\b\2"+
		"\u00ce\u00cc\3\2\2\2\u00cf\u00d2\3\2\2\2\u00d0\u00ce\3\2\2\2\u00d0\u00d1"+
		"\3\2\2\2\u00d1\u00d4\3\2\2\2\u00d2\u00d0\3\2\2\2\u00d3\u00cb\3\2\2\2\u00d3"+
		"\u00d4\3\2\2\2\u00d4\u00d5\3\2\2\2\u00d5\u00d6\7\7\2\2\u00d6\23\3\2\2"+
		"\2\u00d7\u00dd\7\32\2\2\u00d8\u00dd\5\"\22\2\u00d9\u00dd\5\34\17\2\u00da"+
		"\u00dd\5\36\20\2\u00db\u00dd\5 \21\2\u00dc\u00d7\3\2\2\2\u00dc\u00d8\3"+
		"\2\2\2\u00dc\u00d9\3\2\2\2\u00dc\u00da\3\2\2\2\u00dc\u00db\3\2\2\2\u00dd"+
		"\25\3\2\2\2\u00de\u00df\t\7\2\2\u00df\27\3\2\2\2\u00e0\u00e5\5\32\16\2"+
		"\u00e1\u00e2\7\16\2\2\u00e2\u00e4\5\32\16\2\u00e3\u00e1\3\2\2\2\u00e4"+
		"\u00e7\3\2\2\2\u00e5\u00e3\3\2\2\2\u00e5\u00e6\3\2\2\2\u00e6\31\3\2\2"+
		"\2\u00e7\u00e5\3\2\2\2\u00e8\u00e9\t\t\2\2\u00e9\33\3\2\2\2\u00ea\u00eb"+
		"\7/\2\2\u00eb\35\3\2\2\2\u00ec\u00ed\7\60\2\2\u00ed\37\3\2\2\2\u00ee\u00ef"+
		"\7\61\2\2\u00ef!\3\2\2\2\u00f0\u00f1\t\n\2\2\u00f1#\3\2\2\2\37*\62:=D"+
		"GJRW[fimqy\u0086\u008f\u0097\u009a\u009c\u00ac\u00b7\u00c2\u00c4\u00c6"+
		"\u00d0\u00d3\u00dc\u00e5";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}