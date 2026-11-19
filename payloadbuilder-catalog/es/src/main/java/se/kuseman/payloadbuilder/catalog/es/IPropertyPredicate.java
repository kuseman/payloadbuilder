package se.kuseman.payloadbuilder.catalog.es;

import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;

//CSOFF
interface IPropertyPredicate
{
    /** Return the ES property name of this predicate. */
    String getProperty();

    IPredicate getPredicate();

    String getDescription();

    void appendBooleanClause(boolean describe, ElasticStrategy strategy, StringBuilder filterMust, StringBuilder filterMustNot, IExecutionContext context);
}
// CSON
