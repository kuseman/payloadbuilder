package org.kuse.payloadbuilder.core;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.operator.ObjectProjection;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.OperatorBuilder;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.DescribeTableStatement;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.Select;

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

    /**
     * Builds a describe table select
     *
     * <pre>
     * TODO: this needs to be changed and delegated to catalog.
     * For example Elastic there are alot of different documents
     * in the same type
     * </pre>
     **/
    @SuppressWarnings("unused")
    static Pair<Operator, Projection> getDescribeTable(ExecutionContext context, DescribeTableStatement statement)
    {
        throw new NotImplementedException("Not implemented");
    }

    /** Build describe select from provided select */
    static Pair<Operator, Projection> getDescribeSelect(ExecutionContext context, Select select)
    {
        Pair<Operator, Projection> pair = OperatorBuilder.create(context.getSession(), select);
        Operator root = pair.getKey();

        final List<DescribeOperatorRow> describeRows = new ArrayList<>();
        collectOperatorDescribeRows(context, describeRows, root, 0, "", false);

        List<String> describeColumns = new ArrayList<>();
        Map<String, MutableInt> countByColumn = new HashMap<>();

        // Count properties columns
        for (DescribeOperatorRow row : describeRows)
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
        TableAlias alias = TableAliasBuilder
                .of(-1, TableAlias.Type.TABLE, QualifiedName.of("describe"), "d")
                .columns(describeColumns.toArray(EMPTY_STRING_ARRAY))
                .build();

        // Result set rows
        List<Tuple> rows = new ArrayList<>(describeRows.size());
        int pos = 0;
        int size = describeColumns.size();
        for (DescribeOperatorRow dRow : describeRows)
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
            public RowIterator open(ExecutionContext context)
            {
                return RowIterator.wrap(rows.iterator());
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        return Pair.of(describeOperator, getIndexProjection(describeColumns));
    }

    /** Get an object projection over column array */
    static Projection getIndexProjection(List<String> columns)
    {
        return new ObjectProjection(
                columns,
                IntStream.range(0, columns.size()).mapToObj(index -> (Projection) (writer, ctx) ->
                {
                    Tuple tuple = ctx.getTuple();
                    writer.writeValue(tuple.getValue(columns.get(index)));
                }).collect(toList()));
    }

    private static void collectOperatorDescribeRows(
            ExecutionContext context,
            List<DescribeOperatorRow> rows,
            Operator parent,
            int pos,
            String indent,
            boolean last)
    {
        rows.add(new DescribeOperatorRow(parent.getNodeId(), indent + "+- " + parent.getName(), parent.getDescribeProperties(context)));
        String nextIndent = indent + (last ? "   " : "|  ");
        for (int i = 0; i < parent.getChildOperators().size(); i++)
        {
            Operator child = parent.getChildOperators().get(i);
            collectOperatorDescribeRows(context, rows, child, pos + 1, nextIndent, i == parent.getChildOperators().size() - 1);
        }
    }

    /** Describe row */
    private static class DescribeOperatorRow
    {
        final int nodeId;
        final String name;
        final Map<String, Object> properties;

        DescribeOperatorRow(int nodeId, String name, Map<String, Object> properties)
        {
            this.nodeId = nodeId;
            this.name = name;
            this.properties = properties;
        }
    }
}
