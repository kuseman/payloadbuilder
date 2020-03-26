package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;

/** Array projection. Projects a list of sub projections over a selection */
public class ArrayProjection implements Projection
{
    private final List<Projection> projections;
    private final Operator selection;

    public ArrayProjection(List<Projection> projections, Operator selection)
    {
        this.projections = requireNonNull(projections);
        this.selection = requireNonNull(selection);
    }

    @Override
    public void writeValue(OutputWriter writer, OperatorContext context, Row row)
    {
        context.setParentProjectionRow(row);
        int size = projections.size();

        writer.startArray();
        Iterator<Row> it = selection.open(context);
        while (it.hasNext())
        {
            Row selectionRow = it.next();
            for (int i = 0; i < size; i++)
            {
                projections.get(i).writeValue(writer, context, selectionRow);
            }
        }
        writer.endArray();
    }
}
