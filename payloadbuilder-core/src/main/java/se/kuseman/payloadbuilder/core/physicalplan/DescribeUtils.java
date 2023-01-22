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
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.core.common.DescribableNode;

/** Utility class for building a describe result set */
public class DescribeUtils
{
    public static final String LOGICAL_OPERATOR = "Logical operator";
    public static final String POPULATING = "Populating";
    public static final String BATCH_SIZE = "Batch size";
    public static final String OUTER_VALUES = "Outer values";
    public static final String INNER_VALUES = "Inner values";
    public static final String TIME_SPENT = "Time spent";
    public static final String TIME_SPENT_ACC = "Time spent (acc)";
    public static final String EXECUTION_COUNT = "Execution count";
    public static final String PROCESSED_ROWS = "Processed rows";
    public static final String PREDICATE_TIME = "Predicate time";
    public static final String OUTER_HASH_TIME = "Outer hash time";
    public static final String INNER_HASH_TIME = "Inner hash time";

    private DescribeUtils()
    {
    }

    /** Formats a schemas output columns to be used in describe properties */
    public static String getOutputColumns(Schema schema)
    {
        return schema.getColumns()
                .stream()
                .filter(c -> !c.isInternal())
                .map(c -> c.getOutputName() + " ("
                          + c.getType()
                                  .toTypeString()
                          + ")")
                .collect(joining(", "));
    }

    /** Generate a describe tuple vector from provided plan */
    static TupleVector getDescribeVector(IExecutionContext context, DescribableNode node)
    {
        final List<DescribableRow> describeRows = new ArrayList<>();
        collectDescribableRows(context, describeRows, node, 0, "", false);

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
        describeColumns.addAll(0, asList("Node id", "Name"));

        final List<Object[]> rows = new ArrayList<>(describeRows.size());
        int size = describeColumns.size();
        for (DescribableRow row : describeRows)
        {
            Object[] values = new Object[size];
            if (row.nodeId >= 0)
            {
                values[0] = row.nodeId;
            }
            values[1] = row.name;

            for (int i = 2; i < size; i++)
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
                    public Object getValue(int row)
                    {
                        Object value = rows.get(row)[column];
                        return value;
                    }
                };
            }
        };
    }

    private static void collectDescribableRows(IExecutionContext context, List<DescribableRow> rows, DescribableNode parent, int pos, String indent, boolean last)
    {
        rows.add(new DescribableRow(parent.getNodeId(), indent + "+- " + parent.getName(), parent.getDescribeProperties(context)));
        String nextIndent = indent + (last ? "   "
                : "|  ");

        int size = parent.getChildNodes()
                .size();
        for (int i = 0; i < size; i++)
        {
            DescribableNode child = parent.getChildNodes()
                    .get(i);
            collectDescribableRows(context, rows, child, pos + 1, nextIndent, i == parent.getChildNodes()
                    .size() - 1);
        }
    }

    /** Describe row */
    private static class DescribableRow
    {
        final int nodeId;
        final String name;
        final Map<String, Object> properties;

        DescribableRow(int nodeId, String name, Map<String, Object> properties)
        {
            this.nodeId = nodeId;
            this.name = name;
            this.properties = properties;
        }
    }
}
