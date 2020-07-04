package com.viskan.payloadbuilder;

import com.viskan.payloadbuilder.QueryResult.QueryResultMetaData;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorBuilder;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.IfStatement;
import com.viskan.payloadbuilder.parser.PrintStatement;
import com.viskan.payloadbuilder.parser.QualifiedReferenceExpression;
import com.viskan.payloadbuilder.parser.QueryStatement;
import com.viskan.payloadbuilder.parser.SelectStatement;
import com.viskan.payloadbuilder.parser.SetStatement;
import com.viskan.payloadbuilder.parser.Statement;
import com.viskan.payloadbuilder.parser.StatementVisitor;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_OBJECT_ARRAY;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

/** Implementation of {@link QueryResult}.
 * Using a visitor to traverse statements */
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
            System.out.println(operator.toString(1));
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

        currentSelect = null;
        System.out.println(QualifiedReferenceExpression.executionsByName);
        QualifiedReferenceExpression.executionsByName.clear();
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