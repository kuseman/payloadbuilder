package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.EpochDateTime;

/** Literal date time */
public interface ILiteralDateTimeExpression extends IExpression
{
    /** Get value */
    EpochDateTime getValue();
}
