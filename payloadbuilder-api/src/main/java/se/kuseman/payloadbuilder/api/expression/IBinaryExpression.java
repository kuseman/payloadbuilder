package se.kuseman.payloadbuilder.api.expression;

/** Definition of a binary expression */
public interface IBinaryExpression extends IExpression
{
    /** Get left expression */
    IExpression getLeft();

    /** Get right expression */
    IExpression getRight();
}
