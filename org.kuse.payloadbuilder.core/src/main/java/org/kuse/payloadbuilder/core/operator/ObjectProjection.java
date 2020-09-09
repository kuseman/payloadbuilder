package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Projection that projects an object output */
public class ObjectProjection implements Projection
{
    static final Projection[] EMPTY_PROJECTION_ARRAY = new Projection[0];
    private final Operator selection;
    private final List<Projection> projections;
    private final String[] columns;
    private final int length;

    public ObjectProjection(List<String> projectionAliases, List<Projection> projections)
    {
        this(projectionAliases, projections, null);
    }

    ObjectProjection(List<String> projectionAliases, List<Projection> projections, Operator selection)
    {
        if (requireNonNull(projectionAliases, "projectionAliases").size() != requireNonNull(projections, "projections").size())
        {
            throw new IllegalArgumentException("Projection aliases and projections differ in size");
        }

        this.projections = requireNonNull(projections, "projections");
        this.columns = projectionAliases.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
        this.length = columns.length;
        this.selection = selection;
    }

    public String[] getColumns()
    {
        return columns;
    }

    @Override
    public void writeValue(OutputWriter writer, ExecutionContext context)
    {
        Row rowToUse = context.getRow();
        Row prevParentRow = rowToUse;
        if (selection != null)
        {
            Iterator<Row> it = selection.open(context);
            rowToUse = it.hasNext() ? it.next() : null;

            if (rowToUse == null)
            {
                writer.writeValue(null);
                return;
            }
        }

        writer.startObject();
        for (int i = 0; i < length; i++)
        {
            context.setRow(rowToUse);
            writer.writeFieldName(columns[i]);
            projections.get(i).writeValue(writer, context);
        }
        writer.endObject();

        context.setRow(prevParentRow);
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * (selection != null ? selection.hashCode() : 0) +
            37 * projections.hashCode() +
            37 * Arrays.hashCode(columns);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ObjectProjection)
        {
            ObjectProjection that = (ObjectProjection) obj;
            return Objects.equals(selection, that.selection)
                && projections.equals(that.projections)
                && Arrays.equals(columns, that.columns);
        }
        return false;
    }
}
