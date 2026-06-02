package se.kuseman.payloadbuilder.api.catalog;

import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;

/** A sink used on operators when the data flows into catalog. Ie. insert into. etc. */
public interface IDatasink
{
    /**
     * Execute sink with provided input supplier. The supplier creates a fresh {@link TupleIterator} on each call, allowing sinks to decide when — and optionally how many times — to execute the
     * upstream plan. Sinks that cache results (e.g. SELECT INTO with CACHE_TTL) can skip calling the supplier on a cache hit and call it again on later cache refreshes.
     */
    void execute(IExecutionContext context, Supplier<TupleIterator> input);
}
