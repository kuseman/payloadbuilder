package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.Decimal;

/** Literal decimal */
public interface ILiteralDecimalExpression extends IExpression
{
    /** Get value */
    Decimal getValue();
}
