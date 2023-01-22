package se.kuseman.payloadbuilder.api.expression;

/** A dereference expression col.value */
public interface IDereferenceExpression extends IUnaryExpression
{
    /** Return right side of dereference */
    String getRight();
}
