package org.kuse.payloadbuilder.core.parser;

/**
 * Visitor definition of expressions
 *
 * @param <TR> Return type
 * @param <TC> Context type
 */
public interface ExpressionVisitor<TR, TC>
{
    //CSOFF
    TR visit(LiteralNullExpression expression, TC context);

    TR visit(LiteralBooleanExpression expression, TC context);

    TR visit(LiteralIntegerExpression expression, TC context);

    TR visit(LiteralLongExpression expression, TC context);

    TR visit(LiteralFloatExpression expression, TC context);

    TR visit(LiteralDoubleExpression expression, TC context);

    TR visit(LiteralStringExpression expression, TC context);

    TR visit(ComparisonExpression expression, TC context);

    TR visit(ArithmeticUnaryExpression expression, TC context);

    TR visit(ArithmeticBinaryExpression expression, TC context);

    TR visit(LogicalBinaryExpression expression, TC context);

    TR visit(LogicalNotExpression expression, TC context);

    TR visit(InExpression expression, TC context);

    TR visit(LikeExpression expression, TC context);

    TR visit(QualifiedReferenceExpression expression, TC context);

    TR visit(UnresolvedQualifiedReferenceExpression expression, TC context);

    TR visit(NestedExpression expression, TC context);

    TR visit(NullPredicateExpression expression, TC context);

    TR visit(QualifiedFunctionCallExpression expression, TC context);

    TR visit(DereferenceExpression expression, TC context);

    TR visit(UnresolvedDereferenceExpression expression, TC context);

    TR visit(LambdaExpression expression, TC context);

    TR visit(VariableExpression expression, TC context);

    TR visit(SubscriptExpression expression, TC context);

    TR visit(CaseExpression expression, TC context);

    TR visit(UnresolvedSubQueryExpression expression, TC context);
    //CSOFF

}
