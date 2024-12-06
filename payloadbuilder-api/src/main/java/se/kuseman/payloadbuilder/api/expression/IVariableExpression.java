package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Variable expression @var */
public interface IVariableExpression extends IExpression
{
    /** Return name of variable */
    QualifiedName getName();

    /** Return true if this is a system variable otherwise false */
    boolean isSystem();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
