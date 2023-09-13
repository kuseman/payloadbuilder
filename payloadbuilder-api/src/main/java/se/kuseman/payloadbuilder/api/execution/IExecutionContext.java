package se.kuseman.payloadbuilder.api.execution;

import se.kuseman.payloadbuilder.api.execution.vector.IVectorBuilderFactory;
import se.kuseman.payloadbuilder.api.expression.IExpressionFactory;

/** Definition of a execution context. */
public interface IExecutionContext
{
    /** Return the current session */
    IQuerySession getSession();

    /** Return vector builder factory from context */
    IVectorBuilderFactory getVectorBuilderFactory();

    /** Return the statement context */
    IStatementContext getStatementContext();

    /** Return expression factory */
    IExpressionFactory getExpressionFactory();
}
