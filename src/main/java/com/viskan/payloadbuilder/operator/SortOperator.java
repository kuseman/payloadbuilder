package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** Operator sorting target operator */
class SortOperator extends AOperator
{
    private final Operator target;
    private final RowComparator comparator;

    SortOperator(int nodeId, Operator target, RowComparator comparator)
    {
        super(nodeId);
        this.target = requireNonNull(target, "target");
        this.comparator = requireNonNull(comparator, "comparator");
    }

    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        List<Row> rows = new ArrayList<>();
        Iterator<Row> it = target.open(context);
        while (it.hasNext())
        {
            rows.add(it.next());
        }
        Collections.sort(rows, (rowA, rowB) -> comparator.compare(context, rowA, rowB));
        return rows.iterator();
    }
}
