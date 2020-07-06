package com.viskan.payloadbuilder;

import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.ObjectProjection;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorBuilder;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.QualifiedName;
import com.viskan.payloadbuilder.parser.Select;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;

/** Utility class for building a describe result set */
public class DescribeUtils
{
    public static final String LOGICAL_OPERATOR = "Logical operator";
    public static final String POPULATING = "Populating";
    public static final String BATCH_SIZE = "Batch size";
    public static final String PREDICATE = "Predicate";
    public static final String INDEX = "Index";
    public static final String OUTER_VALUES = "Outer values";
    public static final String INNER_VALUES = "Inner values";
    public static final String CATALOG = "Catalog";

    private DescribeUtils()
    {
    }

    /** Build describe select from provided select */
    static Pair<Operator, Projection> getDescribeSelect(QuerySession session, Select select)
    {
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);
        Operator root = pair.getKey();

        final List<DescribeRow> describeRows = new ArrayList<>();
        collectOperatorDescribeRows(describeRows, root, 0, "", false);

        List<String> describeColumns = new ArrayList<>();
        Map<String, MutableInt> countByColumn = new HashMap<>();

        // Count properties columns
        for (DescribeRow row : describeRows)
        {
            for (String col : row.properties.keySet())
            {
                countByColumn.computeIfAbsent(col, k -> new MutableInt()).increment();
                if (!describeColumns.contains(col))
                {
                    describeColumns.add(col);
                }
            }
        }

        // Put properties with the most occurrences first
        describeColumns.sort((a, b) ->
        {
            int c = -1 * (countByColumn.get(a).intValue() - countByColumn.get(b).intValue());
            return c != 0 ? c : String.CASE_INSENSITIVE_ORDER.compare(a, b);
        });

        // Insert first columns
        describeColumns.addAll(0, asList("NodeId", "Name"));
        TableAlias alias = new TableAlias(null, QualifiedName.of("describe"), "d", describeColumns.toArray(EMPTY_STRING_ARRAY));

        // Result set rows
        List<Row> rows = new ArrayList<>(describeRows.size());
        int pos = 0;
        int size = describeColumns.size();
        for (DescribeRow dRow : describeRows)
        {
            Object[] values = new Object[size];
            values[0] = dRow.nodeId;
            values[1] = dRow.name;

            for (int i = 2; i < size; i++)
            {
                values[i] = dRow.properties.get(describeColumns.get(i));
            }

            rows.add(Row.of(alias, pos++, values));
        }

        Operator describeOperator = new Operator()
        {
            @Override
            public Iterator<Row> open(ExecutionContext context)
            {
                return rows.iterator();
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        Projection describeProjection = new ObjectProjection(
                describeColumns,
                IntStream.range(0, describeColumns.size()).mapToObj(index -> (Projection) (writer, context) ->
                {
                    Row row = context.getRow();
                    writer.writeValue(row.getObject(index));
                }).collect(toList()));

        return Pair.of(describeOperator, describeProjection);
    }

    private static void collectOperatorDescribeRows(
            List<DescribeRow> rows,
            Operator parent,
            int pos,
            String indent,
            boolean last)
    {
        //        Object[] values = new Object[DESCRIBE_TABLE_ALIAS.getColumns().length];
        //        values[0] = parent.getNodeId();
        //        values[1] = indent + "+- " + parent.getName();
        //        values[2] = parent.getDescribeString();
        //        values[3] = parent.getDescribeProperties();

        rows.add(new DescribeRow(parent.getNodeId(), indent + "+- " + parent.getName(), parent.getDescribeProperties()));

        indent += last ? "   " : "|  ";
        //        rows.add(Row.of(DESCRIBE_TABLE_ALIAS, pos++, values));

        for (int i = 0; i < parent.getChildOperators().size(); i++)
        {
            Operator child = parent.getChildOperators().get(i);
            collectOperatorDescribeRows(rows, child, pos++, indent, i == parent.getChildOperators().size() - 1);
        }
    }

    private static class DescribeRow
    {
        final int nodeId;
        final String name;
        final Map<String, Object> properties;

        public DescribeRow(int nodeId, String name, Map<String, Object> properties)
        {
            this.nodeId = nodeId;
            this.name = name;
            this.properties = properties;
        }
    }
}
