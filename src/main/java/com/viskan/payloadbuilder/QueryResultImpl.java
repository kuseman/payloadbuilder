package com.viskan.payloadbuilder;

import com.viskan.payloadbuilder.QueryResult.QueryResultMetaData;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.ObjectProjection;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorBuilder;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.DescribeFunctionStatement;
import com.viskan.payloadbuilder.parser.DescribeSelectStatement;
import com.viskan.payloadbuilder.parser.DescribeTableStatement;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.IfStatement;
import com.viskan.payloadbuilder.parser.ParseException;
import com.viskan.payloadbuilder.parser.PrintStatement;
import com.viskan.payloadbuilder.parser.QualifiedName;
import com.viskan.payloadbuilder.parser.QueryStatement;
import com.viskan.payloadbuilder.parser.SelectStatement;
import com.viskan.payloadbuilder.parser.SetStatement;
import com.viskan.payloadbuilder.parser.Statement;
import com.viskan.payloadbuilder.parser.StatementVisitor;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_OBJECT_ARRAY;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Implementation of {@link QueryResult}. Using a visitor to traverse statements
 */
class QueryResultImpl implements QueryResult, QueryResultMetaData, StatementVisitor<Void, Void>
{
    private static final Row DUMMY_ROW = Row.of(TableAlias.of(null, "dummy", "d"), 0, EMPTY_OBJECT_ARRAY);

    private final QuerySession session;
    private final ExecutionContext context;
    private Pair<Operator, Projection> currentSelect;

    /** Queue of statement to process */
    private final List<Statement> queue = new ArrayList<>();

    QueryResultImpl(QuerySession session, QueryStatement query)
    {
        this.session = session;
        this.context = new ExecutionContext(session);
        queue.addAll(query.getStatements());
    }

    @Override
    public Void visit(PrintStatement statement, Void ctx)
    {
        context.clearStatementCache();
        Object value = statement.getExpression().eval(context);
        session.printLine(value);
        return null;
    }

    @Override
    public Void visit(IfStatement statement, Void ctx)
    {
        context.clearStatementCache();
        Object value = statement.getCondition().eval(context);
        if ((Boolean) value)
        {
            queue.addAll(statement.getStatements());
        }
        else
        {
            queue.addAll(statement.getElseStatements());
        }
        return null;
    }

    @Override
    public Void visit(SetStatement statement, Void ctx)
    {
        context.clearStatementCache();
        Object value = statement.getExpression().eval(context);
        context.setVariable(statement.getScope(), statement.getName(), value);
        return null;
    }

    @Override
    public Void visit(DescribeSelectStatement statement, Void ctx)
    {
        currentSelect = DescribeUtils.getDescribeSelect(session, statement.getSelectStatement().getSelect()); //Pair.of(describeOperator, describeProjection);
        return null;
    }

    @Override
    public Void visit(DescribeFunctionStatement statement, Void ctx)
    {
        return null;
    }

    @Override
    public Void visit(DescribeTableStatement statement, Void ctx)
    {
        String catalogAlias = statement.getCatalog();
        QualifiedName tableName = statement.getTableName();
        Catalog catalog = isBlank(catalogAlias) ? session.getDefaultCatalog() : session.getCatalogRegistry().getCatalog(catalogAlias);

        if (catalog == null)
        {
            throw new ParseException("Could not find catalog with alias " + catalogAlias, 0, 0);
        }

        TableAlias tableAlias = TableAlias.of(null, tableName, "");
        Operator operator = catalog.getScanOperator(0, catalogAlias, tableAlias, emptyList());

        context.clear();
        // Get first row from scan operator
        Iterator<Row> iterator = operator.open(context);
        //        List<Row> rows = new ArrayList<>();
        List<Class<?>> typeByColumn = null;

        // Fetch ten first rows from oprator
        // this to minimize the risk of getting null in one columns
        // and don't be able to get data type
        int count = 10;
        int columnCount = -1;
        while (count > 0 && iterator.hasNext())
        {
            if (columnCount == -1)
            {
                columnCount = tableAlias.getColumns().length;
                if (columnCount <= 0)
                {
                    break;
                }
                typeByColumn = new ArrayList<>(Collections.nCopies(columnCount, null));
            }

            Row row = iterator.next();
            count--;

            for (int i = 0; i < columnCount; i++)
            {
                if (typeByColumn.get(i) == null)
                {
                    Object value = row.getObject(i);
                    typeByColumn.set(i, value != null ? value.getClass() : null);
                }
            }

        }

        if (typeByColumn == null)
        {
            return null;
        }

        List<Index> indices = catalog.getIndices(tableName);

        // Name,    Type,   Description
        // art_id,  Column, String
        // index1   Index   [art_id]

        // Create a select over all columns in table

        TableAlias describeAlias = new TableAlias(null, QualifiedName.of("describe"), "d", new String[] {"Name", "Type", "Description"});
        List<Row> describeRows = new ArrayList<>(columnCount + indices.size());
        int pos = 0;
        // Add column rows
        for (int i = 0; i < columnCount; i++)
        {
            Class<?> type = typeByColumn.get(i);
            describeRows.add(Row.of(describeAlias, pos++, new Object[] {tableAlias.getColumns()[i], "Column", type == null ? "Unknown" : type.getSimpleName()}));
        }
        // Add indices
        int i = 1;
        for (Index index : indices)
        {
            describeRows.add(Row.of(describeAlias, pos++, new Object[] {"Index_" + (i++), "Index", index.getColumns() + " (Batch size: " + index.getBatchSize() + ")"}));
        }

        Operator describeOperator = new Operator()
        {
            @Override
            public Iterator<Row> open(ExecutionContext context)
            {
                return describeRows.iterator();
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        Projection describeProjection = new ObjectProjection(
                asList(describeAlias.getColumns()),
                IntStream.range(0, describeAlias.getColumns().length).mapToObj(index -> (Projection) (writer, context) ->
                {
                    Row row = context.getRow();
                    writer.writeValue(row.getObject(index));
                }).collect(toList()));

        currentSelect = Pair.of(describeOperator, describeProjection);
        return null;
    }

    @Override
    public Void visit(SelectStatement statement, Void ctx)
    {
        context.clearStatementCache();
        currentSelect = OperatorBuilder.create(session, statement.getSelect());
        return null;
    }

    private boolean setNext()
    {
        while (currentSelect == null)
        {
            if (queue.isEmpty())
            {
                return false;
            }

            Statement stm = queue.remove(0);
            stm.accept(this, null);
        }

        return true;
    }

    @Override
    public boolean hasMoreResults()
    {
        return setNext();
    }

    @Override
    public void writeResult(OutputWriter writer)
    {
        if (currentSelect == null)
        {
            throw new IllegalArgumentException("No more results");
        }
        Operator operator = currentSelect.getKey();
        Projection projection = currentSelect.getValue();

        context.clear();
        if (operator != null)
        {
            //            System.out.println(operator.toString(1));
            Iterator<Row> iterator = operator.open(context);
            while (iterator.hasNext())
            {
                if (session.abortQuery())
                {
                    break;
                }
                writer.startRow();
                Row row = iterator.next();
                context.setRow(row);
                projection.writeValue(writer, context);
                writer.endRow();
            }
        }
        else
        {
            writer.startRow();
            context.setRow(DUMMY_ROW);
            projection.writeValue(writer, context);
            writer.endRow();
        }

        ObjectWriter printer = new ObjectMapper().writerWithDefaultPrettyPrinter();
        context.getOperatorContext().getNodeData().forEachEntry((i, e) ->
        {
            try
            {
                System.out.println("Node: " + i);
                System.out.println(printer.writeValueAsString(e));
            }
            catch (JsonProcessingException ee)
            {
            }
            return true;
        });

        currentSelect = null;
        //        System.out.println(QualifiedReferenceExpression.executionsByName);
        //        QualifiedReferenceExpression.executionsByName.clear();
    }

    @Override
    public String[] getColumns()
    {
        if (currentSelect == null)
        {
            throw new IllegalArgumentException("No more results");
        }

        return currentSelect.getValue().getColumns();
    }

    @Override
    public QueryResultMetaData getResultMetaData()
    {
        return this;
    }
}
