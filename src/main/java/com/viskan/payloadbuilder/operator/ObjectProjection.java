package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;

/** Projection that projects an object output */
public class ObjectProjection implements Projection
{
    static final Projection[] EMPTY_PROJECTION_ARRAY = new Projection[0];
    private final Operator selection;
    private final Projection[] projections;
    private final String[] columns;
    private final int length;

    public ObjectProjection(Map<String, Projection> projections)
    {
        this(projections, null);
    }

    ObjectProjection(Map<String, Projection> projections, Operator selection)
    {
        this.projections = requireNonNull(projections).values().toArray(EMPTY_PROJECTION_ARRAY);
        this.columns = projections.keySet().toArray(ArrayUtils.EMPTY_STRING_ARRAY);
        this.length = this.columns.length;
        this.selection = selection;
    }

    @Override
    public void writeValue(OutputWriter writer, Row row)
    {
        Row rowToUse = row;

        if (selection != null)
        {
            OperatorContext context = new OperatorContext();
            context.setParentProjectionRow(row);

            Iterator<Row> it = selection.open(context);
            rowToUse = it.hasNext() ? it.next() : null;
        }

        if (rowToUse == null)
        {
            writer.writeValue(null);
            return;
        }

//        Object[] values = new Object[length];
//        Result result = new Result(columns, values);

        writer.startObject();
        for (int i = 0; i < length; i++)
        {
            writer.writeFieldName(columns[i]);
            projections[i].writeValue(writer, rowToUse);
//            values[i] = projections[i].getValue(rowToUse);
        }
        writer.endObject();
//        return result;
    }
}
