package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

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
        this.selection = requireNonNull(selection);
    }

    @Override
    public void writeValue(OutputWriter writer, Row row)
    {
        OperatorContext context = new OperatorContext();
        context.setParentProjectionRow(row);

        int size = projections.size();

//        List<Object[]> result = new ArrayList<>();
        writer.startArray();
        Iterator<Row> it = selection.open(context);
        while (it.hasNext())
        {
            Row selectionRow = it.next();
//            Object[] rowResult = new Object[size];
            for (int i = 0; i < size; i++)
            {
                projections.get(i).writeValue(writer, selectionRow);
                //rowResult[i] = projections.get(i).getValue(selectionRow);
            }
//            result.add(rowResult);
        }
        writer.endArray();
//        return result;
    }
}
