/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
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
     * Builds a describe table select TODO: this needs to be changed and delegated to catalog. For example Elastic there are alot of different objects
     * in a type and since we are fetching the 10 first rows the result will be wrong
     **/
    @SuppressWarnings("unused")
    static Pair<Operator, Projection> getDescribeTable(ExecutionContext context, DescribeTableStatement statement)
    {
        throw new NotImplementedException("Not implemented");
        //        QuerySession session = context.getSession();
        //        String catalogAlias = statement.getCatalog();
        //        QualifiedName tableName = statement.getTableName();
        //        Catalog catalog;
        //        if (isBlank(catalogAlias))
        //        {
        //            catalog = context.getSession().getDefaultCatalog();
        //            catalogAlias = context.getSession().getDefaultCatalogAlias();
        //        }
        //        else
        //        {
        //            catalog = session.getCatalogRegistry().getCatalog(catalogAlias);
        //        }
        //
        //        if (catalog == null)
        //        {
        //            throw new ParseException("Could not find catalog with alias " + catalogAlias, 0, 0);
        //        }
        //
        //        TableAlias tableAlias = TableAlias.of(null, tableName, "");
        //        Operator operator = catalog.getScanOperator(new OperatorData(context.getSession(), 0, catalogAlias, tableAlias, emptyList(), emptyList(), emptyList()));
        //
        //        context.clear();
        //        // Get first row from scan operator
        //        Iterator<Row> iterator = operator.open(context);
        //        List<Class<?>> typeByColumn = null;
        //
        //        // Fetch ten first rows from operator
        //        // This to minimize the risk of getting null in one column
        //        // and don't be able to get data type, it's type less/reflective system :)
        //        int count = 10;
        //        int columnCount = -1;
        //        while (count > 0 && iterator.hasNext())
        //        {
        //            if (columnCount == -1 && tableAlias.getColumns() != null)
        //            {
        //                columnCount = tableAlias.getColumns().length;
        //                if (columnCount <= 0)
        //                {
        //                    break;
        //                }
        //                typeByColumn = new ArrayList<>(Collections.nCopies(columnCount, null));
        //            }
        //
        //            Row row = iterator.next();
        //            count--;
        //
        //            for (int i = 0; i < columnCount; i++)
        //            {
        //                if (typeByColumn.get(i) == null)
        //                {
        //                    Object value = row.getObject(i);
        //                    typeByColumn.set(i, value != null ? value.getClass() : null);
        //                }
        //            }
        //
        //        }
        //
        //        if (typeByColumn == null)
        //        {
        //            return null;
        //        }
        //
        //        List<Index> indices = catalog.getIndices(context.getSession(), catalogAlias, tableName);
        //
        //        // Name,    Type,   Description
        //        // art_id,  Column, String
        //        // index1   Index   [art_id]
        //
        //        // Create a select over all columns in table
        //
        //        TableAlias describeAlias = TableAlias.of(null, QualifiedName.of("describe"), "d", new String[] {"Name", "Type", "Description"});
        //        List<Row> describeRows = new ArrayList<>(columnCount + indices.size());
        //        int pos = 0;
        //        // Add column rows
        //        for (int i = 0; i < columnCount; i++)
        //        {
        //            Class<?> type = typeByColumn.get(i);
        //            describeRows.add(Row.of(describeAlias, pos++, new Object[] {tableAlias.getColumns()[i], "Column", type == null ? "Unknown" : type.getSimpleName()}));
        //        }
        //        describeRows.add(Row.of(describeAlias, pos++, new Object[] {"", "", ""}));
        //        // Add indices
        //        int i = 1;
        //        for (Index index : indices)
        //        {
        //            describeRows.add(Row.of(describeAlias, pos++, new Object[] {"Index_" + (i++), "Index", index.getColumns() + " (Batch size: " + index.getBatchSize() + ")"}));
        //        }
        //
        //        Operator describeOperator = new Operator()
        //        {
        //            @Override
        //            public RowIterator open(ExecutionContext context)
        //            {
        //                return RowIterator.wrap(describeRows.iterator());
        //            }
        //
        //            @Override
        //            public int getNodeId()
        //            {
        //                return 0;
        //            }
        //        };
        //
        //        return Pair.of(describeOperator, getIndexProjection(asList(describeAlias.getColumns())));
    }

    /** Build describe select from provided select */
    static Pair<Operator, Projection> getDescribeSelect(QuerySession session, Select select)
    {
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);
        Operator root = pair.getKey();

        final List<DescribeOperatorRow> describeRows = new ArrayList<>();
        collectOperatorDescribeRows(describeRows, root, 0, "", false);

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
                .of(TableAlias.Type.TABLE, QualifiedName.of("describe"), "d")
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
                    writer.writeValue(tuple.getValue(QualifiedName.of(columns.get(index)), 0));
                }).collect(toList()));
    }

    private static void collectOperatorDescribeRows(
            List<DescribeOperatorRow> rows,
            Operator parent,
            int pos,
            String indent,
            boolean last)
    {
        rows.add(new DescribeOperatorRow(parent.getNodeId(), indent + "+- " + parent.getName(), parent.getDescribeProperties()));
        indent += last ? "   " : "|  ";
        for (int i = 0; i < parent.getChildOperators().size(); i++)
        {
            Operator child = parent.getChildOperators().get(i);
            collectOperatorDescribeRows(rows, child, pos++, indent, i == parent.getChildOperators().size() - 1);
        }
    }

    private static class DescribeOperatorRow
    {
        final int nodeId;
        final String name;
        final Map<String, Object> properties;

        public DescribeOperatorRow(int nodeId, String name, Map<String, Object> properties)
        {
            this.nodeId = nodeId;
            this.name = name;
            this.properties = properties;
        }
    }
}
