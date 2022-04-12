package se.kuseman.payloadbuilder.core.operator;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;
import se.kuseman.payloadbuilder.api.operator.IStatementContext;
import se.kuseman.payloadbuilder.api.operator.NodeData;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/**
 * Context used during a statement.
 *
 * <pre>
 * Contains data that is used during a select etc.
 *
 * Life cycle here is only during one statement, is cleared
 * after completion.
 * </pre>
 */
public class StatementContext implements IStatementContext
{
    /** Stores node unique data by node's unique id */
    private final Map<Integer, NodeData> nodeDataById = new ConcurrentHashMap<>();
    /** Iterator of outer row values used when having an indexed inner operator in Batched operators */
    private Iterator<IOrdinalValues> outerOrdinalValues;
    /** Holder for lambda references during evaluation */
    private List<Object> lambdaValues;
    private ZonedDateTime now = ZonedDateTime.now();
    /** Current row count of previous select statement */
    private int rowCount;
    /** Reference to current context tuple. Used when evaluating expression through out a query */
    private Tuple tuple;

    public StatementContext()
    {
    }

    StatementContext(StatementContext source)
    {
        this.tuple = source.tuple;
    }

    Iterator<IOrdinalValues> getOuterOrdinalValues()
    {
        return outerOrdinalValues;
    }

    void setOuterOrdinalValues(Iterator<IOrdinalValues> outerOrdinalValues)
    {
        this.outerOrdinalValues = outerOrdinalValues;
    }

    /** Clear context state */
    public void clear()
    {
        rowCount = 0;
        tuple = null;
        now = ZonedDateTime.now();
        lambdaValues = null;
        outerOrdinalValues = null;
        nodeDataById.clear();
    }

    public Map<Integer, ? extends NodeData> getNodeData()
    {
        return nodeDataById;
    }

    /** Get node data by id */
    @Override
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getNodeData(Integer nodeId)
    {
        return (T) nodeDataById.get(nodeId);
    }

    /** Get or create node data provided id */
    @Override
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getOrCreateNodeData(Integer nodeId, final Supplier<T> creator)
    {
        // NOTE! If using computeIfAbsent here the calls from AnalyzeOperator/AnalyzeProjection
        // causes ALOT of allocations of LambdaForm's when flight recording, cannot understand why.
        // Might be related to hot code since analyze code is called much more often
        // So this is not thread safe atm. Might introduce locking and change to an array based solution instead
        T res = (T) nodeDataById.get(nodeId);
        if (res == null)
        {
            res = creator.get();
            nodeDataById.put(nodeId, res);
        }
        return res;
    }

    /** Get or create node data provided id */
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getOrCreateNodeData(Integer nodeId)
    {
        return (T) nodeDataById.computeIfAbsent(nodeId, k -> new NodeData());
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

    /** Return current time in local time */
    @Override
    public ZonedDateTime getNow()
    {
        return now;
    }

    public int getRowCount()
    {
        return rowCount;
    }

    public void setRowCount(int rowCount)
    {
        this.rowCount = rowCount;
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

    /** Merge this context's node data with provided context's */
    public void mergeNodeData(StatementContext context)
    {
        // Merge nodes
        for (Entry<Integer, NodeData> e : context.nodeDataById.entrySet())
        {
            NodeData existing = nodeDataById.get(e.getKey());
            if (existing != null)
            {
                existing.merge(e.getValue());
            }
            else
            {
                nodeDataById.put(e.getKey(), e.getValue());
            }
        }
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
