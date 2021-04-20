package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.operator.IIndexValuesFactory.IIndexValues;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/** Operator that groups by a bucket function */
class GroupByOperator extends AOperator
{
    private final Operator operator;
    /** Use an index value factory here for the purpose of creating objects to put in map as key only
     * They implement hashCode/equals so they fit perfectly here. */
    private final IIndexValuesFactory indexValueFactory;
    private final int size;
    private final Map<Integer, Set<String>> columnReferences;

    GroupByOperator(
            int nodeId,
            Operator operator,
            Map<Integer, Set<String>> columnReferences,
            IIndexValuesFactory indexValueFactory,
            int size)
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
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        return ofEntries(true,
                entry("Values", indexValueFactory.toString()));
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        Map<IIndexValues, List<Tuple>> table = new LinkedHashMap<>();
        RowIterator it = operator.open(context);
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            IIndexValues values = indexValueFactory.create(context, tuple);
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

        final Iterator<List<Tuple>> iterator = table.values().iterator();
        //CSOFF
        return new RowIterator()
        //CSON
        {
            TIntObjectMap<TIntSet> groupByOrdinals;

            @Override
            public Tuple next()
            {
                List<Tuple> tuples = iterator.next();
                // Calculate ordinals from the first tuples data
                if (groupByOrdinals == null)
                {
                    groupByOrdinals = new TIntObjectHashMap<>(columnReferences.size());
                    Tuple firstTuple = tuples.get(0);
                    for (Entry<Integer, Set<String>> e : columnReferences.entrySet())
                    {
                        Tuple t = firstTuple.getTuple(e.getKey());
                        TIntSet set = new TIntHashSet(e.getValue().size());
                        e.getValue()
                                .stream()
                                .forEach(c -> set.add(t.getColumnOrdinal(c)));

                        groupByOrdinals.put(e.getKey(), set);
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
            return nodeId == that.nodeId
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
        return String.format("GROUP BY (ID: %d, VALUES: %s)", nodeId, indexValueFactory) + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }
}