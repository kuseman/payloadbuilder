package se.kuseman.payloadbuilder.core;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableAlias.TableAliasBuilder;
import se.kuseman.payloadbuilder.api.operator.NodeData;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.session.CacheProvider;
import se.kuseman.payloadbuilder.core.codegen.BaseProjection;
import se.kuseman.payloadbuilder.core.operator.AObjectOutputWriter;
import se.kuseman.payloadbuilder.core.operator.AObjectOutputWriter.ColumnValue;
import se.kuseman.payloadbuilder.core.operator.ATupleIterator;
import se.kuseman.payloadbuilder.core.operator.ExecutionContext;
import se.kuseman.payloadbuilder.core.operator.OperatorBuilder;
import se.kuseman.payloadbuilder.core.operator.OperatorBuilder.BuildResult;
import se.kuseman.payloadbuilder.core.operator.Projection;
import se.kuseman.payloadbuilder.core.operator.RootProjection;
import se.kuseman.payloadbuilder.core.operator.StatementContext;
import se.kuseman.payloadbuilder.core.operator.TemporaryTable;
import se.kuseman.payloadbuilder.core.parser.AnalyzeStatement;
import se.kuseman.payloadbuilder.core.parser.CacheFlushRemoveStatement;
import se.kuseman.payloadbuilder.core.parser.DescribeSelectStatement;
import se.kuseman.payloadbuilder.core.parser.DescribeTableStatement;
import se.kuseman.payloadbuilder.core.parser.DropTableStatement;
import se.kuseman.payloadbuilder.core.parser.IfStatement;
import se.kuseman.payloadbuilder.core.parser.Option;
import se.kuseman.payloadbuilder.core.parser.ParseException;
import se.kuseman.payloadbuilder.core.parser.PrintStatement;
import se.kuseman.payloadbuilder.core.parser.QueryStatement;
import se.kuseman.payloadbuilder.core.parser.Select;
import se.kuseman.payloadbuilder.core.parser.SelectItem;
import se.kuseman.payloadbuilder.core.parser.SelectStatement;
import se.kuseman.payloadbuilder.core.parser.SetStatement;
import se.kuseman.payloadbuilder.core.parser.ShowStatement;
import se.kuseman.payloadbuilder.core.parser.Statement;
import se.kuseman.payloadbuilder.core.parser.StatementVisitor;
import se.kuseman.payloadbuilder.core.parser.UseStatement;
import se.kuseman.payloadbuilder.core.parser.rewrite.StatementResolver;

/**
 * Implementation of {@link QueryResult}. Using a visitor to traverse statements and executes the query
 */
class QueryResultImpl implements QueryResult, StatementVisitor<Void, Void>
{
    private Pair<Operator, Projection> currentSelect;
    private final QuerySession session;
    private final ExecutionContext context;
    /** Queue of statement to process */
    private final List<Statement> queue = new ArrayList<>();
    private long analyzeQueryTime = -1;

    QueryResultImpl(QuerySession session, QueryStatement query)
    {
        this.session = requireNonNull(session);
        this.context = new ExecutionContext(session);
        queue.addAll(StatementResolver.resolve(query, session)
                .getStatements());
    }

    @Override
    public Void visit(PrintStatement statement, Void ctx)
    {
        Object value = statement.getExpression()
                .eval(context);
        session.printLine(value);
        return null;
    }

    protected Void visit(Statement statement, Void ctx)
    {
        context.getStatementContext()
                .clear();
        statement.accept(this, ctx);
        return null;
    }

    @Override
    public Void visit(IfStatement statement, Void ctx)
    {
        Object value = statement.getCondition()
                .eval(context);
        if (value != null
                && (Boolean) value)
        {
            queue.addAll(0, statement.getStatements());
        }
        else
        {
            queue.addAll(0, statement.getElseStatements());
        }
        return null;
    }

    @Override
    public Void visit(SetStatement statement, Void ctx)
    {
        Object value = statement.getExpression()
                .eval(context);
        if (statement.isSystemProperty())
        {
            session.setSystemProperty(statement.getName(), value);
        }
        else
        {
            context.setVariable(statement.getName(), value);
        }
        return null;
    }

    @Override
    public Void visit(UseStatement statement, Void ctx)
    {
        // Change of default catalog
        if (statement.getExpression() == null)
        {
            session.setDefaultCatalogAlias(statement.getQname()
                    .getFirst());
        }
        // Set property
        else
        {
            QualifiedName qname = statement.getQname();
            String catalogAlias = qname.getFirst();
            QualifiedName property = qname.extract(1);

            String key = join(property.getParts(), ".");
            Object value = statement.getExpression()
                    .eval(context);
            context.getSession()
                    .setCatalogProperty(catalogAlias, key, value);
        }
        return null;
    }

    @Override
    public Void visit(DescribeSelectStatement statement, Void ctx)
    {
        BuildResult buildResult = OperatorBuilder.create(session, statement.getSelectStatement()
                .getSelect());
        currentSelect = DescribeUtils.getDescribeSelect(context, buildResult.getOperator(), buildResult.getProjection());
        return null;
    }

    @Override
    public Void visit(AnalyzeStatement statement, Void ctx)
    {
        SelectStatement selectStatement = statement.getSelectStatement();

        BuildResult buildResult = OperatorBuilder.create(session, selectStatement.getSelect(), true);
        // Write the operator to a noop-writer and collect statistics
        StopWatch sw = StopWatch.createStarted();
        if (selectStatement.isAssignmentSelect())
        {
            applyAssignmentSelect(selectStatement.getSelect(), buildResult);
        }
        else if (selectStatement.getSelect()
                .getInto() != null)
        {
            applySelectInto(selectStatement.getSelect(), buildResult);
        }
        else
        {
            writeInternal(buildResult.getOperator(), buildResult.getProjection(), OutputWriterAdapter.NO_OP_WRITER, null);
        }
        sw.stop();

        Map<Integer, ? extends NodeData> nodeData = context.getStatementContext()
                .getNodeData();

        // Extract the root nodes total time which is the total query time
        long totalQueryTime = sw.getTime(TimeUnit.MILLISECONDS);
        for (NodeData data : nodeData.values())
        {
            data.setTotalQueryTime(totalQueryTime);
        }

        analyzeQueryTime = sw.getTime();

        currentSelect = DescribeUtils.getDescribeSelect(context, buildResult.getOperator(), buildResult.getProjection());
        return null;
    }

    @Override
    public Void visit(ShowStatement statement, Void ctx)
    {
        currentSelect = ShowUtils.getShowSelect(context, statement);
        return null;
    }

    @Override
    public Void visit(CacheFlushRemoveStatement statement, Void ctx)
    {
        CacheProvider provider = context.getSession()
                .getCacheProvider(statement.getType());
        if (statement.isFlush())
        {
            if (statement.isAll())
            {
                provider.flushAll();
            }
            else if (statement.getKey() != null)
            {
                Object key = statement.getKey()
                        .eval(context);
                provider.flush(statement.getName(), key);
            }
            else
            {
                provider.flush(statement.getName());
            }
        }
        // Remove
        else
        {
            if (statement.isAll())
            {
                provider.removeAll();
            }
            else
            {
                provider.remove(statement.getName());
            }
        }

        return null;
    }

    @Override
    public Void visit(DescribeTableStatement statement, Void ctx)
    {
        currentSelect = DescribeUtils.getDescribeTable(context, statement);
        return null;
    }

    @Override
    public Void visit(SelectStatement statement, Void ctx)
    {
        BuildResult buildResult = OperatorBuilder.create(session, statement.getSelect());
        if (statement.isAssignmentSelect())
        {
            applyAssignmentSelect(statement.getSelect(), buildResult);
        }
        else if (statement.getSelect()
                .getInto() != null)
        {
            applySelectInto(statement.getSelect(), buildResult);
        }
        else
        {
            currentSelect = Pair.of(buildResult.getOperator(), buildResult.getProjection());
        }
        return null;
    }

    @Override
    public Void visit(DropTableStatement statement, Void context)
    {
        if (!statement.isTempTable())
        {
            throw new ParseException("DROP TABLE can only be performed on temporary tables", statement.getToken());
        }

        session.dropTemporaryTable(statement.getQname(), statement.isLenient());
        return null;
    }

    private void applySelectInto(Select select, BuildResult buildResult)
    {
        List<Option> options = select.getInto()
                .getOptions();
        QualifiedName cacheName = null;
        Object cacheKey = null;
        Duration ttl = null;

        if (options != null)
        {
            int size = options.size();
            for (int i = 0; i < size; i++)
            {
                Option option = options.get(i);
                String lowerName = lowerCase(option.getOption()
                        .toDotDelimited());
                if (lowerName.equals("cachename"))
                {
                    cacheName = QualifiedName.of(option.getValueExpression()
                            .eval(context));
                }
                else if (lowerName.equals("cachekey"))
                {
                    cacheKey = option.getValueExpression()
                            .eval(context);
                }
                else if (lowerName.equals("cachettl"))
                {
                    ttl = Duration.parse((String) option.getValueExpression()
                            .eval(context));
                }
            }
        }

        TemporaryTable table;
        if (cacheName != null)
        {
            table = context.getSession()
                    .getTempTableCacheProvider()
                    .computIfAbsent(cacheName, cacheKey, ttl, () -> createTempTable(select, buildResult, true));
        }
        else
        {
            table = createTempTable(select, buildResult, false);
        }

        this.context.getSession()
                .setTemporaryTable(table);
    }

    private TemporaryTable createTempTable(Select select, BuildResult buildResult, final boolean cache)
    {
        // Use the select's table source if there is one else create a dummy
        TableAlias tableAlias = select.getFrom() != null ? select.getFrom()
                .getTableSource()
                .getTableAlias()
                : TableAliasBuilder.of(-1, TableAlias.Type.TABLE, select.getInto()
                        .getTable(), "")
                        .build();

        Operator operator = buildResult.getOperator();
        Projection projection = buildResult.getProjection();

        final List<TemporaryTable.Row> rows = new ArrayList<>();
        final MutableObject<List<ColumnValue>> currentRow = new MutableObject<>();
        final MutableObject<String[]> tempTableColumns = new MutableObject<>();

        AObjectOutputWriter writer = new AObjectOutputWriter()
        {
            @Override
            public void initResult(String[] columns)
            {
                tempTableColumns.setValue(columns);
            }

            @Override
            public void consumeRow(List<ColumnValue> row)
            {
                currentRow.setValue(row);
            }
        };

        writeInternal(operator, projection, writer, tuple ->
        {
            List<ColumnValue> row = currentRow.getValue();
            int size = row.size();
            Object[] values = new Object[size];
            String[] columns = tempTableColumns.getValue();
            boolean setColumns = false;

            if (columns.length == 0)
            {
                columns = new String[size];
                tempTableColumns.setValue(columns);
                setColumns = true;
            }

            for (int i = 0; i < size; i++)
            {
                ColumnValue value = row.get(i);
                values[i] = value.getValue();
                if (setColumns)
                {
                    columns[i] = value.getKey();
                }
            }

            if (cache)
            {
                /* Optimize tuple before storing to table */
                context.intern(values);
            }
            rows.add(new TemporaryTable.Row(values));
        });

        return new TemporaryTable(select.getInto()
                .getTable(), tableAlias, tempTableColumns.getValue(), rows);
    }

    private void applyAssignmentSelect(Select select, BuildResult buildResult)
    {
        Operator operator = buildResult.getOperator();
        int size = select.getSelectItems()
                .size();

        int rowCount = 0;
        StatementContext statementContext = context.getStatementContext();

        ATupleIterator iterator = null;
        try
        {
            iterator = new ATupleIterator(operator.open(context));
            while (iterator.hasNext())
            {
                if (session.abortQuery())
                {
                    break;
                }
                Tuple tuple = iterator.next();
                statementContext.setTuple(tuple);
                for (int i = 0; i < size; i++)
                {
                    SelectItem item = select.getSelectItems()
                            .get(i);
                    Object value = item.getAssignmentValue(context);
                    context.setVariable(item.getAssignmentName(), value);
                }
                rowCount++;
            }
        }
        finally
        {
            if (iterator != null)
            {
                iterator.close();
            }
        }
        statementContext.setRowCount(rowCount);
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
            if (stm == null)
            {
                System.err.println();
            }
            // context.clearStatementCache();
            visit(stm, null);
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

        writeInternal(operator, projection, writer, null);
        currentSelect = null;
    }

    // CSOFF
    private void writeInternal(Operator operator, Projection projection, OutputWriter writer, Consumer<Tuple> tupleConsumer)
    // CSON
    {
        int rowCount = 0;
        ATupleIterator iterator = null;
        StatementContext statementContext = context.getStatementContext();
        try
        {
            String[] columns = ArrayUtils.EMPTY_STRING_ARRAY;
            if (projection instanceof RootProjection)
            {
                columns = ((RootProjection) projection).getColumns();
            }
            else if (projection instanceof BaseProjection)
            {
                columns = ((BaseProjection) projection).getProjection()
                        .getColumns();
            }
            writer.initResult(columns);

            // Clear context before opening operator
            statementContext.setTuple(null);
            iterator = new ATupleIterator(operator.open(context));
            while (iterator.hasNext())
            {
                if (session.abortQuery())
                {
                    break;
                }
                Tuple tuple = iterator.next();
                statementContext.setTuple(tuple);
                writer.startRow();
                projection.writeValue(writer, context);
                writer.endRow();
                if (tupleConsumer != null)
                {
                    tupleConsumer.accept(tuple);
                }
                rowCount++;
            }
        }
        finally
        {
            if (iterator != null)
            {
                iterator.close();
            }
        }

        statementContext.setRowCount(rowCount);

        if (analyzeQueryTime != -1)
        {
            context.getSession()
                    .printLine("Query time: " + DurationFormatUtils.formatDurationHMS(analyzeQueryTime));
            analyzeQueryTime = -1;
        }
    }
}