package se.kuseman.payloadbuilder.core;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.cache.Cache;
import se.kuseman.payloadbuilder.core.cache.CacheProvider;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.OutputWriterUtils;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.physicalplan.IPhysicalPlan;
import se.kuseman.payloadbuilder.core.statement.CacheFlushRemoveStatement;
import se.kuseman.payloadbuilder.core.statement.DescribeSelectStatement;
import se.kuseman.payloadbuilder.core.statement.DropTableStatement;
import se.kuseman.payloadbuilder.core.statement.IfStatement;
import se.kuseman.payloadbuilder.core.statement.InsertIntoStatement;
import se.kuseman.payloadbuilder.core.statement.LogicalSelectStatement;
import se.kuseman.payloadbuilder.core.statement.PhysicalSelectStatement;
import se.kuseman.payloadbuilder.core.statement.PrintStatement;
import se.kuseman.payloadbuilder.core.statement.QueryStatement;
import se.kuseman.payloadbuilder.core.statement.SetStatement;
import se.kuseman.payloadbuilder.core.statement.ShowStatement;
import se.kuseman.payloadbuilder.core.statement.Statement;
import se.kuseman.payloadbuilder.core.statement.StatementList;
import se.kuseman.payloadbuilder.core.statement.StatementVisitor;
import se.kuseman.payloadbuilder.core.statement.UseStatement;

/**
 * Interpreter implementation of {@link QueryResult}. Using a visitor to traverse statements and executes the query.
 */
class QueryResultImpl implements QueryResult, StatementVisitor<Void, Void>
{
    /** Queue of statement to process */
    private final List<Statement> queue = new ArrayList<>();
    private final QuerySession session;
    private final ExecutionContext context;

    /** The current plan that will be executed next */
    private IPhysicalPlan currentPlan;

    QueryResultImpl(QuerySession session, QueryStatement queryStatement)
    {
        this.session = requireNonNull(session, "session");
        this.context = new ExecutionContext(session);
        this.queue.addAll(queryStatement.getStatements());
    }

    @Override
    public Void visit(PrintStatement statement, Void ctx)
    {
        ValueVector value = statement.getExpression()
                .eval(TupleVector.CONSTANT, context);
        session.printLine(value.valueAsObject(0));
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
        ValueVector value = statement.getCondition()
                .eval(TupleVector.CONSTANT, context);
        if (value.getPredicateBoolean(0))
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
        ValueVector value = statement.getExpression()
                .eval(TupleVector.CONSTANT, context);
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
        statement.execute(context);
        return null;
    }

    @Override
    public Void visit(CacheFlushRemoveStatement statement, Void ctx)
    {
        CacheProvider provider = context.getSession()
                .getCache(statement.getType());
        if (statement.isFlush())
        {
            if (statement.isAll())
            {
                provider.flushAll();
            }
            else if (statement.getKey() != null)
            {
                ValueVector key = statement.getKey()
                        .eval(TupleVector.CONSTANT, context);

                Cache cache = provider.getCache(statement.getName());
                if (cache != null)
                {
                    cache.flush(key.valueAsObject(0));
                }
            }
            else
            {
                Cache cache = provider.getCache(statement.getName());
                if (cache != null)
                {
                    cache.flush();
                }
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
    public Void visit(StatementList statement, Void context)
    {
        queue.addAll(0, statement.getStatements());
        return null;
    }

    @Override
    public Void visit(PhysicalSelectStatement statement, Void ctx)
    {
        currentPlan = statement.getSelect();
        return null;
    }

    @Override
    public Void visit(DropTableStatement statement, Void ctx)
    {
        session.dropTemporaryTable(statement.getQname(), statement.isLenient());
        return null;
    }

    private boolean setNext()
    {
        while (currentPlan == null)
        {
            if (queue.isEmpty())
            {
                return false;
            }

            Statement stm = queue.remove(0);
            visit(stm, null);

            // Internal plan, write it without output
            if (currentPlan != null
                    && !currentPlan.hasWritableOutput())
            {
                writeInternal(currentPlan, null);
                currentPlan = null;
            }
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
        if (currentPlan == null)
        {
            throw new IllegalArgumentException("No more results");
        }

        writeInternal(currentPlan, writer);
        currentPlan = null;
    }

    // CSOFF
    private void writeInternal(IPhysicalPlan plan, OutputWriter writer)
    // CSON
    {
        StopWatch sw = StopWatch.createStarted();
        int rowCount = 0;
        TupleIterator iterator = null;
        StatementContext statementContext = context.getStatementContext();
        try
        {
            Schema schema = plan.getSchema();
            if (writer != null)
            {
                // Asterisk schema, then we cannot init the result with it since
                // it's not the actual one that will come
                if (SchemaUtils.isAsterisk(schema))
                {
                    writer.initResult(ArrayUtils.EMPTY_STRING_ARRAY);
                }
                else
                {
                    writer.initResult(schema.getColumns()
                            .stream()
                            .filter(c -> !(c instanceof CoreColumn)
                                    || !((CoreColumn) c).isInternal())
                            .map(c ->
                            {
                                String outputName = c.getName();
                                if (c instanceof CoreColumn)
                                {
                                    outputName = ((CoreColumn) c).getOutputName();
                                }
                                return outputName;
                            })
                            .toArray(String[]::new));
                }
            }
            statementContext.setOuterTupleVector(null);

            iterator = plan.execute(context);
            while (iterator.hasNext())
            {
                if (session.abortQuery())
                {
                    break;
                }

                TupleVector tv = iterator.next();

                rowCount += tv.getRowCount();
                if (writer != null)
                {
                    OutputWriterUtils.write(tv, writer, context, true);
                    // Flush after each batch
                    writer.flush();
                }
            }
            if (writer != null)
            {
                writer.endResult();
                writer.flush();
            }
        }
        finally
        {
            if (iterator != null)
            {
                iterator.close();
            }
        }

        // Non writable plans has their own internal state of number of rows
        // most likely the plan above return 0 rows
        if (plan.hasWritableOutput())
        {
            statementContext.setRowCount(rowCount);
        }
        sw.stop();
        long queryTime = sw.getTime(TimeUnit.MILLISECONDS);
        session.setLastQueryExecutionTime(queryTime);
        session.setLastQueryRowCount(rowCount);

        // TODO: Reuse buffer allocator
        // Build a buffer allocator that keeps all allocated buffers in internal lists
        // When we have written the result we can clear the buffer allocator and make it
        // reused for next query
    }

    /* Non executable statements */

    // CSOFF
    @Override
    public Void visit(ShowStatement statement, Void ctx)
    {
        throw new IllegalArgumentException("ShowStatement cannot be executed");
    }

    @Override
    public Void visit(LogicalSelectStatement statement, Void ctx)
    {
        throw new IllegalArgumentException("LogicalSelectStatement cannot be executed");
    }

    @Override
    public Void visit(InsertIntoStatement statement, Void context)
    {
        throw new IllegalArgumentException("InsertIntoStatement cannot be executed");
    }

    @Override
    public Void visit(DescribeSelectStatement statement, Void ctx)
    {
        throw new IllegalArgumentException("DescribeSelectStatement cannot be executed");
    }
    // CSON
}
