package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Caches provided operator to allow rewinds (Used in inner operator for nested loop) */
public class CachingOperator implements Operator
{
    private final Operator target;
    private List<Row> rows = null;

    public CachingOperator(Operator target)
    {
        this.target = requireNonNull(target, "target");
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        if (rows == null)
        {
            rows = new ArrayList<>();
            Iterator<Row> it = target.open(context);
            while (it.hasNext())
            {
                rows.add(it.next());
            }
        }
        return rows.iterator();
    }

    @Override
    public int hashCode()
    {
        return target.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof CachingOperator)
        {
            return target.equals(((CachingOperator) obj).target);
        }
        return super.equals(obj);
    }

    @Override
    public String toString(int indent)
    {
        return "CACHING " + target.toString(indent + 1);
    }
}
