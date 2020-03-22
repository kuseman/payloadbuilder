package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.roaringbitmap.RoaringBitmap;

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
        RoaringBitmap bitmap = new RoaringBitmap();
        List<Row> rows = new ArrayList<>();
        Iterator<Row> it = operator.open(context);
        while (it.hasNext())
        {
            Row row = it.next();
            System.out.println(row.getTableAlias().getTable() + "  " + row.getPos());
            if (bitmap.checkedAdd(row.getPos()))
            {
                rows.add(row);
            }
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
