package se.kuseman.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.DescribableNode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

/** Operator that groups by a bucket function */
class GroupByOperator extends AOperator
{
    private final Operator operator;
    /**
     * Use an index value factory here for the purpose of creating objects to put in map as key only They implement hashCode/equals so they fit perfectly here.
     */
    private final IOrdinalValuesFactory indexValueFactory;
    private final int size;
    private final Map<Integer, Set<String>> columnReferences;

    GroupByOperator(int nodeId, Operator operator, Map<Integer, Set<String>> columnReferences, IOrdinalValuesFactory indexValueFactory, int size)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.columnReferences = requireNonNull(columnReferences, "columnReferences");
        this.indexValueFactory = requireNonNull(indexValueFactory, "indexValueFactory");
        this.size = size;
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(operator);
    }

    @Override
    public String getName()
    {
        return "Group by";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return ofEntries(true, entry("Values", indexValueFactory.toString()));
    }

    @Override
    public TupleIterator open(IExecutionContext context)
    {
        Map<IOrdinalValues, List<Tuple>> table = new LinkedHashMap<>();
        TupleIterator it = operator.open(context);
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            IOrdinalValues values = indexValueFactory.create((ExecutionContext) context, tuple);
            table.compute(values, (k, v) ->
            {
                if (v == null)
                {
                    // Start with singleton
                    return singletonList(tuple);
                }
                else if (v.size() == 1)
                {
                    // Transform into a list
                    List<Tuple> list = new ArrayList<>(2);
                    list.add(v.get(0));
                    list.add(tuple);
                    return list;
                }
                else
                {
                    v.add(tuple);
                    return v;
                }
            });
        }
        it.close();

        final Iterator<List<Tuple>> iterator = table.values()
                .iterator();
        // CSOFF
        return new TupleIterator()
        // CSON
        {
            Int2ObjectMap<IntSet> groupByOrdinals;

            @Override
            public Tuple next()
            {
                List<Tuple> tuples = iterator.next();
                // Calculate ordinals from the first tuples data
                if (groupByOrdinals == null)
                {
                    groupByOrdinals = new Int2ObjectOpenHashMap<>(columnReferences.size());
                    Tuple firstTuple = tuples.get(0);
                    for (Entry<Integer, Set<String>> e : columnReferences.entrySet())
                    {
                        Tuple t = firstTuple.getTuple(e.getKey());
                        IntSet set = new IntOpenHashSet(e.getValue()
                                .size());
                        e.getValue()
                                .stream()
                                .forEach(c -> set.add(t.getColumnOrdinal(c)));

                        groupByOrdinals.put(e.getKey()
                                .intValue(), set);
                    }
                }
                return new GroupedRow(tuples, groupByOrdinals);
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }
        };
    }

    @Override
    public int hashCode()
    {
        return operator.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof GroupByOperator)
        {
            GroupByOperator that = (GroupByOperator) obj;
            return nodeId.equals(that.nodeId)
                    && operator.equals(that.operator)
                    && columnReferences.equals(that.columnReferences)
                    && indexValueFactory.equals(that.indexValueFactory)
                    && size == that.size;
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return String.format("GROUP BY (ID: %d, VALUES: %s)", nodeId, indexValueFactory) + System.lineSeparator() + indentString + operator.toString(indent + 1);
    }
}
