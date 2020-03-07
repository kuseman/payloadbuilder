package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.apache.commons.lang3.StringUtils;

import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * Hash match operator. Hashes outer operator and probes the inner operator
 */
class HashMatch implements Operator
{
    private final Operator outer;
    private final Operator inner;
    private final ToIntFunction<Row> outerHashFunction;
    private final ToIntFunction<Row> innerHashFunction;
    private final Predicate<Row> predicate;
    private final BiFunction<Row, Row, Row> rowMerger;

    HashMatch(
            Operator outer,
            Operator inner,
            ToIntFunction<Row> outerHashFunction,
            ToIntFunction<Row> innerHashFunction,
            Predicate<Row> predicate,
            BiFunction<Row, Row, Row> rowMerger)
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

                    Row prevParent = currentInner.getParent();
                    currentInner.setParent(currentOuter);
                    
                    if (predicate.test(currentInner))
                    {
                        next = rowMerger.apply(currentOuter, currentInner);
                    }
                    else
                    {
                        currentInner.setParent(prevParent);
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
        return "HASH MATCH (outer keys: " + outerHashFunction.toString() + ", inner keys: " + innerHashFunction.toString() + ")" + System.lineSeparator()
            +
            indentString + outer.toString(indent + 1) + System.lineSeparator()
            +
            indentString + inner.toString(indent + 1);
    }
}
