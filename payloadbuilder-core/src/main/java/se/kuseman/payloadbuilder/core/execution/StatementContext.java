package se.kuseman.payloadbuilder.core.execution;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IStatementContext;
import se.kuseman.payloadbuilder.api.execution.NodeData;

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
    private final Map<Integer, NodeData> nodeDataById;
    /** Holder for lambda references during evaluation */
    private List<ValueVector> lambdaValues;
    private ZonedDateTime now;
    /** Current row count of previous select statement */
    private int rowCount;

    /** Reference to the outer tuple used in nested loops to access the looped outer value */
    private TupleVector outerTupleVector;

    public StatementContext()
    {
        nodeDataById = new ConcurrentHashMap<>();
        now = ZonedDateTime.now(ZoneOffset.UTC);
    }

    StatementContext(StatementContext source)
    {
        this.nodeDataById = source.nodeDataById;
        this.lambdaValues = source.lambdaValues;
        this.now = source.now;

    }

    /** Clear context state */
    public void clear()
    {
        rowCount = 0;
        now = ZonedDateTime.now(ZoneOffset.UTC);
        lambdaValues = null;
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
    @Override
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getOrCreateNodeData(Integer nodeId)
    {
        return (T) nodeDataById.computeIfAbsent(nodeId, k -> new NodeData());
    }

    public TupleVector getOuterTupleVector()
    {
        return outerTupleVector;
    }

    public void setOuterTupleVector(TupleVector outerTupleVector)
    {
        this.outerTupleVector = outerTupleVector;
    }

    /** Return current time in local time */
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
    public ValueVector getLambdaValue(int lambdaId)
    {
        if (lambdaValues == null)
        {
            return null;
        }
        ensureSize(lambdaValues, lambdaId);
        return lambdaValues.get(lambdaId);
    }

    /** Set lambda value in scope for provided id */
    public void setLambdaValue(int lambdaId, ValueVector vector)
    {
        if (lambdaValues == null)
        {
            lambdaValues = new ArrayList<>();
        }
        ensureSize(lambdaValues, lambdaId);
        lambdaValues.set(lambdaId, vector);
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
