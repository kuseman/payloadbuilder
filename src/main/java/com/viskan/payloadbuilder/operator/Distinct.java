package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import gnu.trove.set.hash.TLinkedHashSet;

/** Distinct operator.
 * Only returns distinct rows from down stream operator 
 **/
class Distinct implements Operator
{
    private final Operator operator;

    Distinct(Operator operator)
    {
        this.operator = requireNonNull(operator);
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        Set<Row> rows = new TLinkedHashSet<>();
        Iterator<Row> it = operator.open(context);
        while (it.hasNext())
        {
            rows.add(it.next());
        }
        return rows.iterator();
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return "DISTINCT" + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }
}
