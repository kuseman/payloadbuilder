package org.kuse.payloadbuilder.core;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_OBJECT_ARRAY;
import static org.apache.commons.lang3.StringUtils.join;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.ObjectProjection;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.OperatorBuilder;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.CacheFlushStatement;
import org.kuse.payloadbuilder.core.parser.CacheRemoveStatement;
import org.kuse.payloadbuilder.core.parser.DescribeSelectStatement;
import org.kuse.payloadbuilder.core.parser.DescribeTableStatement;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.IfStatement;
import org.kuse.payloadbuilder.core.parser.PrintStatement;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QueryStatement;
import org.kuse.payloadbuilder.core.parser.Select;
import org.kuse.payloadbuilder.core.parser.SelectItem;
import org.kuse.payloadbuilder.core.parser.SelectStatement;
import org.kuse.payloadbuilder.core.parser.SetStatement;
import org.kuse.payloadbuilder.core.parser.ShowStatement;
import org.kuse.payloadbuilder.core.parser.Statement;
import org.kuse.payloadbuilder.core.parser.StatementVisitor;
import org.kuse.payloadbuilder.core.parser.UseStatement;

/**
 * Implementation of {@link QueryResult}. Using a visitor to traverse statements
 */
class QueryResultImpl implements QueryResult, StatementVisitor<Void, Void>
{
    private static final Row DUMMY_ROW = Row.of(TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("dummy"), "d").build(), 0, EMPTY_OBJECT_ARRAY);
    private final QuerySession session;
    private final CatalogRegistry registry;
    private ExecutionContext context;
    private final QueryStatement query;
    private Pair<Operator, Projection> currentSelect;

    /** Queue of statement to process */
    private final List<Statement> queue = new ArrayList<>();

    QueryResultImpl(QuerySession session, QueryStatement query)
    {
        this.session = requireNonNull(session);
        this.registry = session.getCatalogRegistry();
        this.query = query;
        this.context = new ExecutionContext(session);
        queue.addAll(query.getStatements());
    }

    @Override
    public void reset()
    {
        queue.clear();
        queue.addAll(query.getStatements());
        context = new ExecutionContext(session);
    }

    @Override
    public Void visit(PrintStatement statement, Void ctx)
    {
        Object value = statement.getExpression().eval(context);
        session.printLine(value);
        return null;
    }

    @Override
    public Void visit(IfStatement statement, Void ctx)
    {
        Object value = statement.getCondition().eval(context);
        if (value != null && (Boolean) value)
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
        Object value = statement.getExpression().eval(context);
        context.setVariable(statement.getName(), value);
        return null;
    }

    @Override
    public Void visit(UseStatement statement, Void ctx)
    {
        // Change of default catalog
        if (statement.getExpression() == null)
        {
            registry.setDefaultCatalog(statement.getQname().getFirst());
        }
        // Set property
        else
        {
            QualifiedName qname = statement.getQname();
            String catalogAlias = qname.getFirst();
            QualifiedName property = qname.extract(1);

            String key = join(property.getParts(), ".");
            Object value = statement.getExpression().eval(context);
            context.getSession().setCatalogProperty(catalogAlias, key, value);
        }
        return null;
    }

    @Override
    public Void visit(DescribeSelectStatement statement, Void ctx)
    {
        currentSelect = DescribeUtils.getDescribeSelect(context, statement.getSelectStatement().getSelect());
        return null;
    }

    @Override
    public Void visit(ShowStatement statement, Void ctx)
    {
        context.clear();
        currentSelect = ShowUtils.createShowOperator(context, statement);
        return null;
    }

    @Override
    public Void visit(CacheFlushStatement statement, Void ctx)
    {
        if (statement.isAll())
        {
            // NOT Supported!
            return null;
        }

        if (statement.getKey() != null)
        {
            Object key = statement.getKey().eval(context);
            context.getSession().getTupleCacheProvider().flushCache(statement.getName(), key);
        }
        else
        {
            context.getSession().getTupleCacheProvider().flushCache(statement.getName());
        }
        return null;
    }

    @Override
    public Void visit(CacheRemoveStatement statement, Void ctx)
    {
        context.getSession().getTupleCacheProvider().removeCache(statement.getName());
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
        if (statement.isAssignmentSelect())
        {
            applyAssignmentSelect(statement.getSelect());
        }
        else
        {
            currentSelect = OperatorBuilder.create(session, statement.getSelect());
        }
        return null;
    }

    private void applyAssignmentSelect(Select select)
    {
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);

        Operator operator = pair.getKey();
        int size = select.getSelectItems().size();

        if (operator != null)
        {
            RowIterator iterator = null;
            try
            {
                iterator = operator.open(context);
                Tuple tuple = null;
                while (iterator.hasNext())
                {
                    if (session.abortQuery())
                    {
                        break;
                    }
                    tuple = iterator.next();
                    context.setTuple(tuple);

                    for (int i = 0; i < size; i++)
                    {
                        SelectItem item = select.getSelectItems().get(i);
                        Object value = item.getAssignmentValue(context);
                        context.setVariable(item.getAssignmentName(), value);
                    }
                }
            }
            finally
            {
                if (iterator != null)
                {
                    iterator.close();
                }
            }
        }
        else
        {
            context.setTuple(DUMMY_ROW);

            for (int i = 0; i < size; i++)
            {
                SelectItem item = select.getSelectItems().get(i);
                Object value = item.getAssignmentValue(context);
                context.setVariable(item.getAssignmentName(), value);
            }
        }
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
            // context.clearStatementCache();
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

        String[] columns = ArrayUtils.EMPTY_STRING_ARRAY;
        if (projection instanceof ObjectProjection)
        {
            columns = ((ObjectProjection) projection).getColumns();
        }
        writer.initResult(columns);

        context.clear();
        if (operator != null)
        {
            RowIterator iterator = null;
            try
            {
                iterator = operator.open(context);
                while (iterator.hasNext())
                {
                    if (session.abortQuery())
                    {
                        break;
                    }
                    writer.startRow();
                    Tuple tuple = iterator.next();
                    context.setTuple(tuple);
                    projection.writeValue(writer, context);
                    writer.endRow();
                }
            }
            finally
            {
                if (iterator != null)
                {
                    iterator.close();
                }
            }
        }
        else
        {
            writer.startRow();
            context.setTuple(DUMMY_ROW);
            projection.writeValue(writer, context);
            writer.endRow();
        }

        currentSelect = null;
    }
}
