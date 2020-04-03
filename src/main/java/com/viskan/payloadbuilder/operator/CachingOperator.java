package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.repeat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Caches provided operator to allow rewinds (Used in inner operator for nested loop) */
public class CachingOperator implements Operator
{
    private final Operator operator;
    private List<Row> rows = null;

    public CachingOperator(Operator target)
    {
        this.operator = requireNonNull(target, "operator");
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        if (rows == null)
        {
            rows = new ArrayList<>();
            Iterator<Row> it = operator.open(context);
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
        return operator.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof CachingOperator)
        {
            return operator.equals(((CachingOperator) obj).operator);
        }
        return super.equals(obj);
    }

    @Override
    public String toString(int indent)
    {
        String indentString = repeat("  ", indent);
        return "CACHING" + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }
}
