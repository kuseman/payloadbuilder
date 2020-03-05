package com.viskan.payloadbuilder.parser.tree;

/** Visitor definition of expressions */
public interface ExpressionVisitor<TR, TC>
{
    TR visit(LiteralNullExpression expression, TC context);
    TR visit(LiteralBooleanExpression expression, TC context);
    TR visit(LiteralNumericExpression expression, TC context);
    TR visit(LiteralDecimalExpression expression, TC context);
    TR visit(LiteralStringExpression expression, TC context);
    TR visit(ComparisonExpression expression, TC context);
    TR visit(ArithmeticUnaryExpression expression, TC context);
    TR visit(ArithmeticBinaryExpression expression, TC context);
    TR visit(LogicalBinaryExpression expression, TC context);
    TR visit(LogicalNotExpression expression, TC context);
    TR visit(InExpression expression, TC context);
    TR visit(QualifiedReferenceExpression expression, TC context);
    TR visit(NestedExpression expression, TC context);
    TR visit(NullPredicateExpression expression, TC context);
    TR visit(QualifiedFunctionCallExpression expression, TC context);
    TR visit(DereferenceExpression expression, TC context);
    TR visit(LambdaExpression expression, TC context);
}
