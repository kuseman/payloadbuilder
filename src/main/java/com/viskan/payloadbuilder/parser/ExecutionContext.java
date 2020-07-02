package com.viskan.payloadbuilder.parser;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.Row;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Context used during execution of a query */
public class ExecutionContext
{
    private final QuerySession session;
    /** Holder for lambda references during evaluation */
    private final List<Object> lambdaValues = new ArrayList<>();
    private final long now = System.currentTimeMillis();
//    private final EvaluationContext evaluationContext = new EvaluationContext();
    private final OperatorContext operatorContext = new OperatorContext();
    
    /** Reference to row. Used in projections, correlated sub queries */
    private Row row;
    
    public ExecutionContext(QuerySession session)
    {
        this.session = requireNonNull(session, "session");
    }
    
    /** Get current row */
    public Row getRow()
    {
        return row;
    }
    
    /** Set current row */
    public void setRow(Row row)
    {
        this.row = row;
    }
    
    /** Return session */
    public QuerySession getSession()
    {
        return session;
    }
    
    /** Return current time in local time */
    public long getNow()
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
        ensureSize(lambdaValues, lambdaId);
        return lambdaValues.get(lambdaId);
    }

    /** Set lambda value in scope for provided id */
    public void setLambdaValue(int lambdaId, Object value)
    {
        ensureSize(lambdaValues, lambdaId);
        lambdaValues.set(lambdaId, value);
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
