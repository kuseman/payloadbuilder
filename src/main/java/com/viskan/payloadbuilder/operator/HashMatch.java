package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.ToIntBiFunction;

import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.commons.lang3.StringUtils;

/**
 * Hash match operator. Hashes outer operator and probes the inner operator
 */
public class HashMatch implements Operator
{
    private final String logicalOperator;
    private final Operator outer;
    private final Operator inner;
    private final ToIntBiFunction<EvaluationContext, Row> outerHashFunction;
    private final ToIntBiFunction<EvaluationContext, Row> innerHashFunction;
    private final BiPredicate<EvaluationContext, Row> predicate;
    private final RowMerger rowMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;

    /* Statistics */
    private int executionCount;

    public HashMatch(
            String logicalOperator,
            Operator outer,
            Operator inner,
            ToIntBiFunction<EvaluationContext, Row> outerHashFunction,
            ToIntBiFunction<EvaluationContext, Row> innerHashFunction,
            BiPredicate<EvaluationContext, Row> predicate,
            RowMerger rowMerger,
            boolean populating,
            boolean emitEmptyOuterRows)
    {
        this.logicalOperator = requireNonNull(logicalOperator, "logicalOperator");
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.outerHashFunction = requireNonNull(outerHashFunction, "outerHashFunction");
        this.innerHashFunction = requireNonNull(innerHashFunction, "innerHashFunction");
        this.predicate = requireNonNull(predicate, "predicate");
        this.rowMerger = requireNonNull(rowMerger, "rowMerger");
        this.populating = populating;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        executionCount++;
        Map<IntKey, List<Row>> table = hash(context);
        if (table.isEmpty())
        {
            return emptyIterator();
        }

        boolean markOuterRows = populating || emitEmptyOuterRows;
        Iterator<Row> probeIterator = probeIterator(context.getParentRow(), table, context, markOuterRows);

        if (populating)
        {
            // Populate inner rows
            while (probeIterator.hasNext())
            {
                probeIterator.next();
            }

            return tableIterator(table, emitEmptyOuterRows ? TableIteratorType.BOTH : TableIteratorType.MATCHED);
        }

        if (!emitEmptyOuterRows)
        {
            return probeIterator;
        }

        // Left join
        // 1. Probe matched rows
        // 2. Probe non matched rows from table
        //        return posOrderIterator(probeIterator, tableIterator(table, false));
        return new IteratorChain(
                probeIterator,
                tableIterator(table, TableIteratorType.NON_MATCHED));
    };

    /**
     * NOTE! Special key class used in hash map. To avoid allocations and use int as hashcode for rows A instance of this class is used for all rows.
     * Since hashcode is all that is needed to find potential matching rows this technique is used
     **/
    private static class IntKey
    {
        private int key;

        @Override
        public int hashCode()
        {
            return key;
        }

        @Override
        public boolean equals(Object obj)
        {
            return true;
        }
    }

    private Map<IntKey, List<Row>> hash(OperatorContext context)
    {
        IntKey key = new IntKey();
        Map<IntKey, List<Row>> table = new LinkedHashMap<>();
        Iterator<Row> oi = outer.open(context);
        while (oi.hasNext())
        {
            Row row = oi.next();
            key.key = outerHashFunction.applyAsInt(context.getEvaluationContext(), row);
            List<Row> list = table.get(key);
            if (list == null)
            {
                // Start with singleton list
                list = singletonList(row);
                table.put(key, list);
                continue;
            }
            else if (list.size() == 1)
            {
                // Convert to array list
                list = new ArrayList<>(list);
                table.put(key, list);
            }
            list.add(row);
        }
        return table;
    }

    private Iterator<Row> probeIterator(
            Row contextParent,
            Map<IntKey, List<Row>> table,
            OperatorContext context,
            boolean markOuterRows)
    {
        final Iterator<Row> ii = inner.open(context);
        return new Iterator<Row>()
        {
            Row next;
            Row currentInner;
            List<Row> outerList;
            int outerIndex;
            IntKey key = new IntKey();

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public Row next()
            {
                Row r = next;
                next = null;
                return r;
            }

            boolean setNext()
            {
                while (next == null)
                {
                    if (currentInner == null)
                    {
                        if (!ii.hasNext())
                        {
                            return false;
                        }

                        currentInner = ii.next();

                        key.key = innerHashFunction.applyAsInt(context.getEvaluationContext(), currentInner);
                        List<Row> list = table.get(key);
                        if (list == null)
                        {
                            currentInner = null;
                            continue;
                        }
                        outerList = list;
                        outerIndex = 0;
                        continue;
                    }

                    if (outerIndex >= outerList.size())
                    {
                        outerList = null;
                        currentInner = null;
                        continue;
                    }

                    Row currentOuter = outerList.get(outerIndex++);
                    currentOuter.setPredicateParent(contextParent);
                    currentInner.setPredicateParent(currentOuter);

                    if (predicate.test(context.getEvaluationContext(), currentInner))
                    {
                        next = rowMerger.merge(currentOuter, currentInner, populating);
                        if (markOuterRows)
                        {
                            currentOuter.match = true;
                        }
                    }

                    currentInner.clearPredicateParent();
                    currentOuter.clearPredicateParent();
                }

                return true;
            }
        };
    }

    private Iterator<Row> tableIterator(
            Map<IntKey, List<Row>> table,
            TableIteratorType type)
    {
        final Iterator<List<Row>> tableIt = table.values().iterator();
        return new Iterator<Row>()
        {
            private Row next;
            private List<Row> list;
            private int index;

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public Row next()
            {
                Row result = next;
                next = null;
                return result;
            }

            boolean setNext()
            {
                while (next == null)
                {
                    if (list == null)
                    {
                        if (!tableIt.hasNext())
                        {
                            return false;
                        }

                        list = tableIt.next();
                        index = 0;
                        continue;
                    }
                    else if (index >= list.size())
                    {
                        list = null;
                        continue;
                    }

                    Row row = list.get(index++);
                    if ((type == TableIteratorType.MATCHED && row.match)
                        ||
                        type == TableIteratorType.NON_MATCHED && !row.match
                        ||
                        type == TableIteratorType.BOTH)
                    {
                        next = row;
                    }
                    row.match = false;
                }
                return true;
            }
        };
    }

    private enum TableIteratorType
    {
        MATCHED,
        NON_MATCHED,
        BOTH;
    }

    @Override
    public int hashCode()
    {
        return 17 + (outer.hashCode() * 37) + (inner.hashCode() * 37);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof HashMatch)
        {
            HashMatch that = (HashMatch) obj;
            return outer.equals(that.outer)
                &&
                inner.equals(that.inner)
                &&
                outerHashFunction.equals(that.outerHashFunction)
                &&
                innerHashFunction.equals(that.innerHashFunction)
                &&
                predicate.equals(that.predicate)
                &&
                rowMerger.equals(that.rowMerger)
                &&
                populating == that.populating
                &&
                emitEmptyOuterRows == that.emitEmptyOuterRows;
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String desc = String.format("HASH MATCH (%s) (POPULATING: %s, OUTER: %s, EXECUTION COUNT: %s, OUTER KEYS: %s, INNER KEYS: %s, PREDICATE: %s)",
                logicalOperator,
                populating,
                emitEmptyOuterRows,
                executionCount,
                outerHashFunction.toString(),
                innerHashFunction.toString(),
                predicate.toString());
        return desc + System.lineSeparator()
            +
            indentString + outer.toString(indent + 1) + System.lineSeparator()
            +
            indentString + inner.toString(indent + 1);
    }
}
