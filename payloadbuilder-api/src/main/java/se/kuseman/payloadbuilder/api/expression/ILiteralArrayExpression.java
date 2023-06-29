package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Literal array */
public interface ILiteralArrayExpression extends IExpression
{
    /** Return the Array value */
    ValueVector getValue();
}
