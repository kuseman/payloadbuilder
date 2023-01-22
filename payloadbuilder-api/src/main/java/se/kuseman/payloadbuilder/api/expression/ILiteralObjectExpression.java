package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.ObjectVector;

/** Literal object */
public interface ILiteralObjectExpression extends IExpression
{
    /** Return the Object value */
    ObjectVector getValue();
}
