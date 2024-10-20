package se.kuseman.payloadbuilder.core.execution;

import static java.util.stream.Collectors.toList;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.IStatementContext;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

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

    private long now;
    private long nowUtc;

    /** Current row count of previous select statement */
    private int rowCount;

    /** Reference to the outer tuple used in nested loops to access the looped outer value */
    private TupleVector outerTupleVector;

    /** Reference to outer tuple used in index seek operations where the inner operator picks it's values from. */
    private TupleVector indexSeekTupleVector;
    /** Cache of seeks keys for index seek predicate. Used to avoid multiple calculations if called twice. */
    private List<ISeekPredicate.ISeekKey> indexSeekKeys;

    public StatementContext()
    {
        nodeDataById = new ConcurrentHashMap<>();
        setNow();
    }

    StatementContext(StatementContext source)
    {
        this.nodeDataById = source.nodeDataById;
        this.lambdaValues = source.lambdaValues;
        this.now = source.now;

    }

    private void setNow()
    {
        Instant now = Instant.now();

        this.nowUtc = now.toEpochMilli();

        long diff = (ZoneId.systemDefault()
                .getRules()
                .getOffset(now)
                .getLong(ChronoField.OFFSET_SECONDS) * 1000);

        this.now = nowUtc + diff;
    }

    /** Clear context state */
    public void clear()
    {
        rowCount = 0;
        setNow();
        lambdaValues = null;
        nodeDataById.clear();
        indexSeekTupleVector = null;
        indexSeekKeys = null;
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

    @SuppressWarnings("unchecked")
    @Override
    public <T extends NodeData> List<T> getNodeDatas(Class<T> nodeClass)
    {
        return nodeDataById.values()
                .stream()
                .filter(n -> n.getClass() == nodeClass)
                .map(n -> (T) n)
                .collect(toList());
    }

    public TupleVector getOuterTupleVector()
    {
        return outerTupleVector;
    }

    public void setOuterTupleVector(TupleVector outerTupleVector)
    {
        this.outerTupleVector = outerTupleVector;
    }

    public TupleVector getIndexSeekTupleVector()
    {
        return indexSeekTupleVector;
    }

    /** Set index seek tuple into context */
    public void setIndexSeekTupleVector(TupleVector indexSeekTupleVector)
    {
        if (indexSeekTupleVector.getRowCount() == 0)
        {
            throw new IllegalArgumentException("Index seek tuple must have rows");
        }

        this.indexSeekTupleVector = indexSeekTupleVector;
        this.indexSeekKeys = null;
    }

    public List<ISeekPredicate.ISeekKey> getIndexSeekKeys()
    {
        return indexSeekKeys;
    }

    public void setIndexSeekKeys(List<ISeekKey> indexSeekKeys)
    {
        this.indexSeekKeys = indexSeekKeys;
    }

    /**
     * Return context epoch millis (UTC) for now NOTE! This is now for when the context was created since all accesses to now during a statement should be the same
     */
    public long getNow()
    {
        return now;
    }

    public long getNowUtc()
    {
        return nowUtc;
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

    /** Traverse all node datas and perform provided action */
    public void forEachNodeData(BiConsumer<Integer, NodeData> action)
    {
        nodeDataById.forEach(action);
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
