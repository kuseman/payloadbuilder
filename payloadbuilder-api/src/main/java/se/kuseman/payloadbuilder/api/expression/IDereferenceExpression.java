package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** A dereference expression col.value */
public interface IDereferenceExpression extends IUnaryExpression
{
    /** Return right side of dereference */
    String getRight();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    default QualifiedName getQualifiedColumn()
    {
        QualifiedName qname = getExpression().getQualifiedColumn();
        if (qname != null)
        {
            // Combine the qname with right
            return qname.extend(getRight());
        }
        return null;
    }
}
