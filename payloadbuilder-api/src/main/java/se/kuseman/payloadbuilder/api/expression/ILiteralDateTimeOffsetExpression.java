package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;

/** Literal date time offset */
public interface ILiteralDateTimeOffsetExpression extends IExpression
{
    /** Get value */
    EpochDateTimeOffset getValue();
}
