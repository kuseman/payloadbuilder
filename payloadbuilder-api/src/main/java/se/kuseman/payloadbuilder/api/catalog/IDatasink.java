package se.kuseman.payloadbuilder.api.catalog;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;

/** A sink used on operators when the data flows into catalog. Ie. insert into. etc. */
public interface IDatasink
{
    /** Execute sink with provided input {@link TupleIterator}. */
    void execute(IExecutionContext context, TupleIterator input);
}
