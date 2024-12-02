package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableInt;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.common.DescribableNode;

/** Utility class for building a describe result set */
public class DescribeUtils
{
    public static final String LOGICAL_OPERATOR = "Logical Operator";
    public static final String POPULATING = "Populating";
    public static final String BATCH_SIZE = "Batch Size";
    public static final String BATCH_COUNT = "Batch Count";
    public static final String OUTER_VALUES = "Outer Values";
    public static final String INNER_VALUES = "Inner Values";
    public static final String TIME_SPENT = "Time Spent";
    public static final String TIME_SPENT_ACC = "Time Spent (acc)";
    public static final String EXECUTION_COUNT = "Execution Count";
    public static final String PROCESSED_ROWS = "Processed Rows";
    public static final String PREDICATE_TIME = "Predicate Time";
    public static final String OUTER_HASH_TIME = "Outer Hash Time";
    public static final String INNER_HASH_TIME = "Inner Hash Time";

    private DescribeUtils()
    {
    }

    /** Formats a schemas output columns to be used in describe properties */
    public static String getOutputColumns(Schema schema)
    {
        return schema.getColumns()
                .stream()
                .filter(c -> !(c instanceof CoreColumn)
                        || !((CoreColumn) c).isInternal())
                .map(c ->
                {
                    String output = c.getName();
                    if (c instanceof CoreColumn)
                    {
                        output = ((CoreColumn) c).getOutputName();
                    }
                    return output + " ("
                           + c.getType()
                                   .toTypeString()
                           + ")";
                })
                .collect(joining(", "));
    }

    /** Generate a describe tuple vector from provided plan */
    static TupleVector getDescribeVector(IExecutionContext context, DescribableNode node)
    {
        final List<DescribableRow> describeRows = new ArrayList<>();
        collectDescribableRows(context, describeRows, -1, node, 0, "", false);

        List<String> describeColumns = new ArrayList<>();
        Map<String, MutableInt> countByColumn = new HashMap<>();

        // Count properties columns
        for (DescribableRow row : describeRows)
        {
            for (String col : row.properties.keySet())
            {
                countByColumn.computeIfAbsent(col, k -> new MutableInt())
                        .increment();
                if (!describeColumns.contains(col))
                {
                    describeColumns.add(col);
                }
            }
        }

        // Put properties with the most occurrences first
        describeColumns.sort((a, b) ->
        {
            int c = -1 * (countByColumn.get(a)
                    .intValue()
                    - countByColumn.get(b)
                            .intValue());
            return c != 0 ? c
                    : String.CASE_INSENSITIVE_ORDER.compare(a, b);
        });

        // Insert first columns
        describeColumns.addAll(0, asList("Node Id", "Parent Node Id", "Name"));

        final List<Object[]> rows = new ArrayList<>(describeRows.size());
        int size = describeColumns.size();
        for (DescribableRow row : describeRows)
        {
            Object[] values = new Object[size];
            if (row.nodeId >= 0)
            {
                values[0] = row.nodeId;
            }
            if (row.parentNodeId >= 0)
            {
                values[1] = row.parentNodeId;
            }
            values[2] = row.name;

            for (int i = 3; i < size; i++)
            {
                values[i] = row.properties.get(describeColumns.get(i));
            }

            rows.add(values);
        }

        final int rowCount = rows.size();
        final Schema schema = new Schema(describeColumns.stream()
                .map(c -> new Column(c, ResolvedType.of(Type.Any)))
                .collect(toList()));

        return new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public int getRowCount()
            {
                return rowCount;
            }

            @Override
            public ValueVector getColumn(int column)
            {
                return new ValueVector()
                {

                    @Override
                    public ResolvedType type()
                    {
                        return ResolvedType.of(Type.Any);
                    }

                    @Override
                    public int size()
                    {
                        return rowCount;
                    }

                    @Override
                    public boolean isNull(int row)
                    {
                        return rows.get(row)[column] == null;
                    }

                    @Override
                    public Object getAny(int row)
                    {
                        Object value = rows.get(row)[column];
                        return value;
                    }
                };
            }
        };
    }

    private static void collectDescribableRows(IExecutionContext context, List<DescribableRow> rows, int parentNodeId, DescribableNode parent, int pos, String indent, boolean last)
    {
        rows.add(new DescribableRow(parent.getNodeId(), parentNodeId, indent + "+- " + parent.getName(), parent.getDescribeProperties(context)));
        String nextIndent = indent + (last ? "   "
                : "|  ");
        int size = parent.getChildNodes()
                .size();
        for (int i = 0; i < size; i++)
        {
            DescribableNode child = parent.getChildNodes()
                    .get(i);
            collectDescribableRows(context, rows, parent.getNodeId(), child, pos + 1, nextIndent, i == parent.getChildNodes()
                    .size() - 1);
        }
    }

    /** Describe row */
    private static class DescribableRow
    {
        final int nodeId;
        final int parentNodeId;
        final String name;
        final Map<String, Object> properties;

        DescribableRow(int nodeId, int parentNodeId, String name, Map<String, Object> properties)
        {
            this.nodeId = nodeId;
            this.parentNodeId = parentNodeId;
            this.name = name;
            this.properties = properties;
        }
    }
}
