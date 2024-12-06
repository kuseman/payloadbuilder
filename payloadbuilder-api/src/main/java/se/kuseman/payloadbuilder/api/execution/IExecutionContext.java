package se.kuseman.payloadbuilder.api.execution;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.execution.vector.IVectorFactory;
import se.kuseman.payloadbuilder.api.expression.IExpressionFactory;

/** Definition of a execution context. */
public interface IExecutionContext
{
    /** Return the current session */
    IQuerySession getSession();

    /** Return vector builder factory from context */
    IVectorFactory getVectorFactory();

    /** Return the statement context */
    IStatementContext getStatementContext();

    /** Return expression factory */
    IExpressionFactory getExpressionFactory();

    /** Return value of provided variable name. */
    ValueVector getVariableValue(QualifiedName qname);
}
