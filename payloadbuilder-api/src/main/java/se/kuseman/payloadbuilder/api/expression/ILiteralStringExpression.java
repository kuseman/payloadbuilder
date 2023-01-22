package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.catalog.UTF8String;

/** Literal stringILiteralDoubleExpression.java */
public interface ILiteralStringExpression extends IExpression
{
    /** Get value */
    UTF8String getValue();
}
