package se.kuseman.payloadbuilder.core.statement;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;

/** Base class for select statements */
public abstract class SelectStatement extends Statement
{
    /** Execute select statement */
    public abstract TupleIterator execute(IExecutionContext context);
}
