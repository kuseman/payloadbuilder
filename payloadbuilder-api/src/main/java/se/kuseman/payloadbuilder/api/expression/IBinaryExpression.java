package se.kuseman.payloadbuilder.api.expression;

/** Definition of a binary expression */
public abstract interface IBinaryExpression extends IExpression
{
    /** Get left expression */
    IExpression getLeft();

    /** Get right expression */
    IExpression getRight();
}
