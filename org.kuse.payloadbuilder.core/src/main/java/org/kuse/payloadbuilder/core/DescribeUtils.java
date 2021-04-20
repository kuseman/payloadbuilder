package org.kuse.payloadbuilder.core;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
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
import org.kuse.payloadbuilder.core.operator.DescribableNode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.RootProjection;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.DescribeTableStatement;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

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
    static Pair<Operator, Projection> getDescribeSelect(ExecutionContext context, Operator operator, Projection projection)
    {
        final List<DescribableRow> describeRows = new ArrayList<>();
        describeRows.add(new DescribableRow(-1, "Selection", emptyMap()));
        collectDescribableRows(context, describeRows, operator, 0, "", false);
        describeRows.add(new DescribableRow(-1, "Projection", emptyMap()));
        collectDescribableRows(context, describeRows, projection, 0, "", false);

        List<String> describeColumns = new ArrayList<>();
        Map<String, MutableInt> countByColumn = new HashMap<>();

        // Count properties columns
        for (DescribableRow row : describeRows)
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
        describeColumns.addAll(0, asList("Node id", "Name"));
        TableAlias alias = TableAliasBuilder
                .of(-1, TableAlias.Type.TABLE, QualifiedName.of("describe"), "d")
                .build();

        String[] columns = describeColumns.toArray(EMPTY_STRING_ARRAY);
        // Result set rows
        List<Tuple> rows = new ArrayList<>(describeRows.size());
        int pos = 0;
        int size = describeColumns.size();
        for (DescribableRow dRow : describeRows)
        {
            Object[] values = new Object[size];
            if (dRow.nodeId >= 0)
            {
                values[0] = dRow.nodeId;
            }
            values[1] = dRow.name;

            for (int i = 2; i < size; i++)
            {
                values[i] = dRow.properties.get(describeColumns.get(i));
            }

            rows.add(Row.of(alias, pos++, columns, values));
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
        return new RootProjection(
                columns,
                IntStream.range(0, columns.size()).mapToObj(index -> (Projection) (writer, ctx) ->
                {
                    Tuple tuple = ctx.getStatementContext().getTuple();
                    writer.writeValue(tuple.getValue(index));
                }).collect(toList()));
    }

    private static void collectDescribableRows(
            ExecutionContext context,
            List<DescribableRow> rows,
            DescribableNode parent,
            int pos,
            String indent,
            boolean last)
    {
        rows.add(new DescribableRow(parent.getNodeId(), indent + "+- " + parent.getName(), parent.getDescribeProperties(context)));
        String nextIndent = indent + (last ? "   " : "|  ");
        for (int i = 0; i < parent.getChildNodes().size(); i++)
        {
            DescribableNode child = parent.getChildNodes().get(i);
            collectDescribableRows(context, rows, child, pos + 1, nextIndent, i == parent.getChildNodes().size() - 1);
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
