package se.kuseman.payloadbuilder.core.statement;

import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;

/** Base class for select statements */
public abstract class SelectStatement extends Statement
{
    /** Execute select statement */
    public abstract TupleIterator execute(IExecutionContext context);
}
