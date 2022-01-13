package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ProjectionCode;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Projection used for the root select */
public class RootProjection implements Projection
{
    private static final Projection[] EMPTY = new Projection[0];
    private final String[] columns;
    private final Projection[] projections;

    public RootProjection(List<String> columns, List<Projection> projections)
    {
        this.columns = requireNonNull(columns, "columns").toArray(ArrayUtils.EMPTY_STRING_ARRAY);
        this.projections = requireNonNull(projections, "projections").toArray(EMPTY);
    }

    @Override
    public String getName()
    {
        return "Root";
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return asList(projections);
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        List<String> cols = new ArrayList<>(columns.length);
        for (int i = 0; i < columns.length; i++)
        {
            if (projections[i].isAsterisk())
            {
                cols.add("*");
            }
            else if (isBlank(columns[i]))
            {
                cols.add("''");
            }
            else
            {
                cols.add(columns[i]);
            }
        }

        return MapUtils.ofEntries(MapUtils.entry("Columns", cols));
    }

    @Override
    public ProjectionCode generateCode(CodeGeneratorContext context)
    {
        ProjectionCode code = context.getProjectionCode();
        StringBuilder sb = new StringBuilder();

        sb.append("writer.startObject();\n");
        sb.append("Tuple rootTuple = context.getStatementContext().getTuple();\n");

        int size = projections.length;
        for (int i = 0; i < size; i++)
        {
            Projection projection = projections[i];

            if (!(projection.isAsterisk()))
            {
                sb.append("writer.writeFieldName(\"").append(columns[i]).append("\");\n");
            }

            if (i > 0)
            {
                // Re-set context tuple on each iterator since it can change with nested projections
                sb.append("context.getStatementContext().setTuple(rootTuple);\n");
            }
            context.setTupleFieldName("rootTuple");
            sb.append(projection.generateCode(context).getCode());
            sb.append(System.lineSeparator());
        }

        sb.append("writer.endObject();\n");
        code.setCode(sb.toString());
        return code;
    }

    @Override
    public void writeValue(OutputWriter writer, ExecutionContext context)
    {
        // The root projection is always an object
        writer.startObject();

        Tuple tuple = context.getStatementContext().getTuple();
        int size = projections.length;
        for (int i = 0; i < size; i++)
        {
            Projection projection = projections[i];

            // An asterisk selection has no column so skip
            if (!(projection.isAsterisk()))
            {
                writer.writeFieldName(columns[i]);
            }

            if (i > 0)
            {
                // Re-set context tuple on each iterator since it can change with nested projections
                context.getStatementContext().setTuple(tuple);
            }
            projection.writeValue(writer, context);
        }

        writer.endObject();
    }

    /** Get columns for this root projection */
    public String[] getColumns()
    {
        int size = projections.length;
        int asteriskIndex = -1;
        List<String> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            if (projections[i].isAsterisk())
            {
                asteriskIndex = i;
                continue;
            }
            else if (asteriskIndex != -1
                && i < size)
            {
                return ArrayUtils.EMPTY_STRING_ARRAY;
            }
            result.add(columns[i]);
        }

        return result.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    }

    Projection[] getProjections()
    {
        return projections;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(projections);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof RootProjection)
        {
            RootProjection that = (RootProjection) obj;
            return Arrays.equals(columns, that.columns)
                && Arrays.equals(projections, that.projections);
        }
        return false;
    }

    @Override
    public String toString()
    {
        int size = columns.length;
        return IntStream.range(0, size)
                .mapToObj(i -> columns[i] + " = " + projections[i])
                .collect(joining(System.lineSeparator()));
    }
}
