package se.kuseman.payloadbuilder.api.operator;

import se.kuseman.payloadbuilder.api.session.IQuerySession;

/** Definition of a execution context. */
public interface IExecutionContext
{
    /** Return the current session */
    IQuerySession getSession();

    /** Return the statement context */
    IStatementContext getStatementContext();

    void intern(Object[] optimizedValues);
}
