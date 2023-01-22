package se.kuseman.payloadbuilder.core.expression;

import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Marker interface used on expressions that can be inverted (ie. a NOT before the expression) */
public interface Invertable
{
    /** Return the inverted variant of this expression */
    IExpression getInvertedExpression();
}
