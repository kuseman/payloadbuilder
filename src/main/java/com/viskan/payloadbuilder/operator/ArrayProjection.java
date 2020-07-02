package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.OutputWriter;
import com.viskan.payloadbuilder.parser.ExecutionContext;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;

/** Array projection. Projects a list of sub projections over a selection */
class ArrayProjection implements Projection
{
    private final List<Projection> projections;
    private final Operator selection;

    ArrayProjection(List<Projection> projections, Operator selection)
    {
        this.projections = requireNonNull(projections);
        this.selection = selection;
    }

    @Override
    public void writeValue(OutputWriter writer, ExecutionContext context)
    {
        int size = projections.size();

        writer.startArray();
        if (selection != null)
        {
            Row prevRow = context.getRow();
            Iterator<Row> it = selection.open(context);
            while (it.hasNext())
            {
                Row row = it.next();
                for (int i = 0; i < size; i++)
                {
                    context.setRow(row);
                    projections.get(i).writeValue(writer, context);
                }
            }
            context.setRow(prevRow);
        }
        else
        {
            for (int i = 0; i < size; i++)
            {
                projections.get(i).writeValue(writer, context);
            }
        }
        writer.endArray();
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
