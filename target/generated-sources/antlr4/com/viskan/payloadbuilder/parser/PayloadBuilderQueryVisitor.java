// Generated from PayloadBuilderQuery.g4 by ANTLR 4.7.1

//CSOFF
//@formatter:off
package com.viskan.payloadbuilder.parser;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link PayloadBuilderQueryParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface PayloadBuilderQueryVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery(PayloadBuilderQueryParser.QueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#selectItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectItem(PayloadBuilderQueryParser.SelectItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#nestedSelectItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNestedSelectItem(PayloadBuilderQueryParser.NestedSelectItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#sortItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSortItem(PayloadBuilderQueryParser.SortItemContext ctx);
	/**
	 * Visit a parse tree produced by the {@code primaryExpression}
	 * labeled alternative in {@link PayloadBuilderQueryParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimaryExpression(PayloadBuilderQueryParser.PrimaryExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link PayloadBuilderQueryParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalNot(PayloadBuilderQueryParser.LogicalNotContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inExpression}
	 * labeled alternative in {@link PayloadBuilderQueryParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInExpression(PayloadBuilderQueryParser.InExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code comparisonExpression}
	 * labeled alternative in {@link PayloadBuilderQueryParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparisonExpression(PayloadBuilderQueryParser.ComparisonExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link PayloadBuilderQueryParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticBinary(PayloadBuilderQueryParser.ArithmeticBinaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link PayloadBuilderQueryParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticUnary(PayloadBuilderQueryParser.ArithmeticUnaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link PayloadBuilderQueryParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullPredicate(PayloadBuilderQueryParser.NullPredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link PayloadBuilderQueryParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalBinary(PayloadBuilderQueryParser.LogicalBinaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link PayloadBuilderQueryParser#primary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDereference(PayloadBuilderQueryParser.DereferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code lambdaExpression}
	 * labeled alternative in {@link PayloadBuilderQueryParser#primary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLambdaExpression(PayloadBuilderQueryParser.LambdaExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link PayloadBuilderQueryParser#primary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnReference(PayloadBuilderQueryParser.ColumnReferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link PayloadBuilderQueryParser#primary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubscript(PayloadBuilderQueryParser.SubscriptContext ctx);
	/**
	 * Visit a parse tree produced by the {@code nestedExpression}
	 * labeled alternative in {@link PayloadBuilderQueryParser#primary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNestedExpression(PayloadBuilderQueryParser.NestedExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code functionCallExpression}
	 * labeled alternative in {@link PayloadBuilderQueryParser#primary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionCallExpression(PayloadBuilderQueryParser.FunctionCallExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code literalExpression}
	 * labeled alternative in {@link PayloadBuilderQueryParser#primary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLiteralExpression(PayloadBuilderQueryParser.LiteralExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#functionCall}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionCall(PayloadBuilderQueryParser.FunctionCallContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#literal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLiteral(PayloadBuilderQueryParser.LiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#compareOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCompareOperator(PayloadBuilderQueryParser.CompareOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#qname}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQname(PayloadBuilderQueryParser.QnameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(PayloadBuilderQueryParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#numericLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumericLiteral(PayloadBuilderQueryParser.NumericLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#decimalLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDecimalLiteral(PayloadBuilderQueryParser.DecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#stringLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLiteral(PayloadBuilderQueryParser.StringLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link PayloadBuilderQueryParser#booleanLiteral}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanLiteral(PayloadBuilderQueryParser.BooleanLiteralContext ctx);
}