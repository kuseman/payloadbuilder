package se.kuseman.payloadbuilder.core.operator;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.core.QuerySession;

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

    /** Variables in context */
    private Map<String, Object> variables;
    /** Intern cache. Used during optimization of tuples to reduce duplicate values. Used before stuff is put to cache */
    private Map<Object, Object> internCache;
    // /** Catalog properties that only live during one execution of a query */
    // private Map<String, Map<String, Object>> catalogProperties;

    public ExecutionContext(QuerySession session)
    {
        this.session = requireNonNull(session, "session");
        // Copy session variables if any
        this.variables = session.getVariables() != null ? new HashMap<>(session.getVariables())
                : null;
        this.statementContext = new StatementContext();
    }

    private ExecutionContext(ExecutionContext source)
    {
        this.session = source.session;
        this.variables = source.variables;
        this.internCache = source.internCache;
        this.statementContext = new StatementContext(source.statementContext);
    }

    /** Return session */
    @Override
    public QuerySession getSession()
    {
        return session;
    }

    /** Get variables map */
    public Map<String, Object> getVariables()
    {
        return defaultIfNull(variables, emptyMap());
    }

    /** Set variable to context */
    public void setVariable(String name, Object value)
    {
        if (variables == null)
        {
            variables = new HashMap<>();
        }
        variables.put(name, value);
    }

    /** Get variable from context */
    public Object getVariableValue(String name)
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

    /**
     * Intern array values. Used when optimizing tuples to reduce duplicated values when values are put to cache etc.
     */
    @Override
    public void intern(Object[] array)
    {
        if (internCache == null)
        {
            internCache = new HashMap<>();
        }

        int length = array.length;
        for (int i = 0; i < length; i++)
        {
            final Object value = array[i];
            array[i] = internCache.computeIfAbsent(value, Function.identity());
        }
    }

    /** Copy this context */
    public ExecutionContext copy()
    {
        return new ExecutionContext(this);
    }
}
