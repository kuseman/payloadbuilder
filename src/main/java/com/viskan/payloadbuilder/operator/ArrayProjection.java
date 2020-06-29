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
        this.selection = selection;
    }

    @Override
    public void writeValue(OutputWriter writer, OperatorContext context, Row row)
    {
        Row prevParentRow = context.getParentRow();
        context.setParentRow(row);
        int size = projections.size();

        writer.startArray();
        if (selection != null)
        {
            Iterator<Row> it = selection.open(context);
            while (it.hasNext())
            {
                Row selectionRow = it.next();
                for (int i = 0; i < size; i++)
                {
                    projections.get(i).writeValue(writer, context, selectionRow);
                }
            }
        }
        else
        {
            for (int i = 0; i < size; i++)
            {
                projections.get(i).writeValue(writer, context, row);
            }
        }
        writer.endArray();
        context.setParentRow(prevParentRow);
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * selection.hashCode() +
            37 * projections.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ArrayProjection)
        {
            ArrayProjection p = (ArrayProjection) obj;
            return selection.equals(p.selection)
                && projections.equals(p.projections);
        }
        return false;
    }
}
