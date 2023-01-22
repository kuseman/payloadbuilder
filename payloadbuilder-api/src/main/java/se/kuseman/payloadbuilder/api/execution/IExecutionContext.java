package se.kuseman.payloadbuilder.api.execution;

/** Definition of a execution context. */
public interface IExecutionContext
{
    /** Return the current session */
    IQuerySession getSession();

    /** Return the statement context */
    IStatementContext getStatementContext();
}
