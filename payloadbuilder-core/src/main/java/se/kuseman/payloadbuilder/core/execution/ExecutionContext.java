package se.kuseman.payloadbuilder.core.execution;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.getIfNull;

import java.util.HashMap;
import java.util.Map;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IVectorFactory;
import se.kuseman.payloadbuilder.api.expression.IExpressionFactory;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator;
import se.kuseman.payloadbuilder.core.execution.vector.VectorFactory;

/**
 * Context used during execution of a query
 *
 * <pre>
 * Life cycle is during a whole query and all it's statements
 * </pre>
 */
public class ExecutionContext implements IExecutionContext
{
    private final QuerySession session;
    private final StatementContext statementContext;
    private final VectorFactory vectorFactory;
    private final ExpressionFactory expressionFactory;

    /** Variables in context */
    private Map<String, ValueVector> variables;

    public ExecutionContext(QuerySession session)
    {
        this.session = requireNonNull(session, "session");
        // Copy session variables if any
        this.variables = session.getVariables() != null ? new HashMap<>(session.getVariables())
                : null;
        this.statementContext = new StatementContext();
        this.vectorFactory = session.getVectorFactory();
        this.expressionFactory = new ExpressionFactory();
    }

    private ExecutionContext(ExecutionContext source)
    {
        this.session = source.session;
        this.variables = source.variables;
        this.statementContext = new StatementContext(source.statementContext);
        this.vectorFactory = source.vectorFactory;
        this.expressionFactory = new ExpressionFactory();
    }

    /** Return session */
    @Override
    public QuerySession getSession()
    {
        return session;
    }

    @Override
    public IVectorFactory getVectorFactory()
    {
        return vectorFactory;
    }

    @Override
    public IExpressionFactory getExpressionFactory()
    {
        return expressionFactory;
    }

    public BufferAllocator getBufferAllocator()
    {
        return vectorFactory.getAllocator();
    }

    /** Get variables map */
    public Map<String, ValueVector> getVariables()
    {
        return getIfNull(variables, emptyMap());
    }

    /** Set variable to context */
    public void setVariable(String name, ValueVector value)
    {
        if (variables == null)
        {
            variables = new HashMap<>();
        }
        variables.put(name, value);
    }

    /** Get variable from context */
    @Override
    public ValueVector getVariableValue(String name)
    {
        return variables != null ? variables.get(name)
                : null;
    }

    @Override
    public StatementContext getStatementContext()
    {
        return statementContext;
    }

    public String getVersionString()
    {
        return ExecutionContext.class.getPackage()
                .getImplementationVersion();
    }

    /** Copy this context. This is a shallow copy. */
    public ExecutionContext copy()
    {
        return new ExecutionContext(this);
    }
}
