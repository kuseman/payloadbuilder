package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

import org.apache.commons.lang3.StringUtils;

import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * Hash match operator. Hashes outer operator and probes the inner operator
 */
class HashJoin implements Operator
{
    private final Operator outer;
    private final Operator inner;
    private final ToIntFunction<Row> outerHashFunction;
    private final ToIntFunction<Row> innerHashFunction;
    private final BiPredicate<EvaluationContext, Row> predicate;
    private final RowMerger rowMerger;

    HashJoin(
            Operator outer,
            Operator inner,
            ToIntFunction<Row> outerHashFunction,
            ToIntFunction<Row> innerHashFunction,
            BiPredicate<EvaluationContext, Row> predicate,
            RowMerger rowMerger)
    {
        this.outer = outer;
        this.inner = inner;
        this.outerHashFunction = outerHashFunction;
        this.innerHashFunction = innerHashFunction;
        this.predicate = predicate;
        this.rowMerger = rowMerger;
    }

    private TIntObjectHashMap<List<Row>> hash(OperatorContext context)
    {
        TIntObjectHashMap<List<Row>> table = new TIntObjectHashMap<>();
        Iterator<Row> oi = outer.open(context);
        while (oi.hasNext())
        {
            Row row = oi.next();
            int hash = outerHashFunction.applyAsInt(row);
            List<Row> list = table.get(hash);
            if (list == null)
            {
                // Start with singleton list
                list = singletonList(row);
                table.put(hash, list);
                continue;
            }
            else if (list.size() == 1)
            {
                // Convert to array list
                list = new LinkedList<>(list);
                table.put(hash, list);
            }
            list.add(row);
        }
        return table;
    }

    private Iterator<Row> probeIterator(TIntObjectHashMap<List<Row>> table, OperatorContext context)
    {
        final Iterator<Row> ii = inner.open(context);
        
        // If populating -> probe and then iterate table
        
        return new Iterator<Row>()
        {
            Row next;
            Row currentInner;
            Iterator<Row> outerIt;

            @Override
            public boolean hasNext()
            {
                return next != null || setNext();
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

                        int hash = innerHashFunction.applyAsInt(currentInner);
                        List<Row> list = table.get(hash);
                        if (list == null)
                        {
                            currentInner = null;
                            continue;
                        }
                        outerIt = list.iterator();
                        continue;
                    }

                    if (!outerIt.hasNext())
                    {
                        outerIt = null;
                        currentInner = null;
                        continue;
                    }

                    Row currentOuter = outerIt.next();
                    if (currentInner.evaluatePredicate(currentOuter, context.getEvaluationContext(), predicate))
                    {
                        next = rowMerger.merge(currentOuter, currentInner, false);
                    }
                }

                return next != null;
            }
        };
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        TIntObjectHashMap<List<Row>> table = hash(context);
        if (table.isEmpty())
        {
            List<Row> l = emptyList();
            return l.iterator();
        }

        return probeIterator(table, context);
    };

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return "HASH JOIN (outer keys: " + outerHashFunction.toString() + ", inner keys: " + innerHashFunction.toString() + ")" + System.lineSeparator()
            +
            indentString + outer.toString(indent + 1) + System.lineSeparator()
            +
            indentString + inner.toString(indent + 1);
    }
}