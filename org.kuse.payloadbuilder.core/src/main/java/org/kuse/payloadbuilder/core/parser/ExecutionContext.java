package org.kuse.payloadbuilder.core.parser;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.operator.OperatorContext;
import org.kuse.payloadbuilder.core.operator.Tuple;

/** Context used during execution of a query */
public class ExecutionContext
{
    public static final String ROW_COUNT = "rowcount";

    private final QuerySession session;
    /** Holder for lambda references during evaluation */
    private List<Object> lambdaValues;
    private Map<String, Object> variables;
    private final ZonedDateTime now = ZonedDateTime.now();
    private final OperatorContext operatorContext = new OperatorContext();

    /** Reference to tuple. Used in projections, correlated sub queries */
    private Tuple tuple;

    public ExecutionContext(QuerySession session)
    {
        this.session = requireNonNull(session, "session");
        // Copy session variables if any
        this.variables = session.getVariables() != null ? new HashMap<>(session.getVariables()) : null;
    }

    /** Get current tuple */
    public Tuple getTuple()
    {
        return tuple;
    }

    /** Set current tuple */
    public void setTuple(Tuple tuple)
    {
        this.tuple = tuple;
    }

    /** Return session */
    public QuerySession getSession()
    {
        return session;
    }

    /** Return current time in local time */
    public ZonedDateTime getNow()
    {
        return now;
    }

    /** Clear temporary data. Used between statements */
    public void clear()
    {
        operatorContext.clear();
    }

    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    /** Get lambda value in scope for provided id */
    public Object getLambdaValue(int lambdaId)
    {
        if (lambdaValues == null)
        {
            return null;
        }
        ensureSize(lambdaValues, lambdaId);
        return lambdaValues.get(lambdaId);
    }

    /** Set lambda value in scope for provided id */
    public void setLambdaValue(int lambdaId, Object value)
    {
        if (lambdaValues == null)
        {
            lambdaValues = new ArrayList<>();
        }
        ensureSize(lambdaValues, lambdaId);
        lambdaValues.set(lambdaId, value);
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
        return variables != null ? variables.get(lowerCase(name)) : null;
    }

    private void ensureSize(List<?> list, int itemIndex)
    {
        // size = 2, index = 0, 1
        int size = list.size();
        if (size > itemIndex)
        {
            return;
        }

        // size 2, index = 2
        int diff = itemIndex + 1 - size;
        list.addAll(Collections.nCopies(diff, null));
    }
}
