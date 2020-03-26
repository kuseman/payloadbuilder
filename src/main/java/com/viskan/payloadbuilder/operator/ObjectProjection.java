package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;

import avro.shaded.com.google.common.base.Objects;

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

    public ObjectProjection(Map<String, Projection> projections, Operator selection)
    {
        this.projections = requireNonNull(projections).values().toArray(EMPTY_PROJECTION_ARRAY);
        this.columns = projections.keySet().toArray(ArrayUtils.EMPTY_STRING_ARRAY);
        this.length = this.columns.length;
        this.selection = selection;
    }

    @Override
    public void writeValue(OutputWriter writer, OperatorContext context, Row row)
    {
        Row rowToUse = row;

        if (selection != null)
        {
            context.setParentProjectionRow(row);
            Iterator<Row> it = selection.open(context);
            rowToUse = it.hasNext() ? it.next() : null;
        }

        if (rowToUse == null)
        {
            writer.writeValue(null);
            return;
        }

        writer.startObject();
        for (int i = 0; i < length; i++)
        {
            writer.writeFieldName(columns[i]);
            projections[i].writeValue(writer, context, rowToUse);
        }
        writer.endObject();
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * (selection != null ? selection.hashCode() : 0) +
            37 * Arrays.hashCode(projections) +
            37 * Arrays.hashCode(columns);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ObjectProjection)
        {
            ObjectProjection p = (ObjectProjection) obj;
            return Objects.equal(selection, p.selection)
                &&
                Arrays.equals(projections, p.projections)
                &&
                Arrays.equals(columns, p.columns);
        }
        return false;
    }
}
