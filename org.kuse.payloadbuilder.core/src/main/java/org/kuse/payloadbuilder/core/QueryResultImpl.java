package org.kuse.payloadbuilder.core;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_OBJECT_ARRAY;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.join;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.FunctionInfo;
import org.kuse.payloadbuilder.core.operator.ObjectProjection;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.OperatorBuilder;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.DescribeSelectStatement;
import org.kuse.payloadbuilder.core.parser.DescribeTableStatement;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.IfStatement;
import org.kuse.payloadbuilder.core.parser.ParseException;
import org.kuse.payloadbuilder.core.parser.PrintStatement;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QueryStatement;
import org.kuse.payloadbuilder.core.parser.Select;
import org.kuse.payloadbuilder.core.parser.SelectItem;
import org.kuse.payloadbuilder.core.parser.SelectStatement;
import org.kuse.payloadbuilder.core.parser.SetStatement;
import org.kuse.payloadbuilder.core.parser.ShowStatement;
import org.kuse.payloadbuilder.core.parser.ShowStatement.Type;
import org.kuse.payloadbuilder.core.parser.Statement;
import org.kuse.payloadbuilder.core.parser.StatementVisitor;
import org.kuse.payloadbuilder.core.parser.UseStatement;

/**
 * Implementation of {@link QueryResult}. Using a visitor to traverse statements
 */
class QueryResultImpl implements QueryResult, StatementVisitor<Void, Void>
{
    private static final Row DUMMY_ROW = Row.of(TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("dummy"), "d").build(), 0, EMPTY_OBJECT_ARRAY);
    private static final TableAlias SHOW_VARIABLES_ALIAS = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("variables"), "v").columns(new String[] {"Name", "Value"}).build();
    private static final TableAlias SHOW_TABLES_ALIAS = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tables"), "t").columns(new String[] {"Name"}).build();
    private static final TableAlias SHOW_FUNCTIONS_ALIAS = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("functions"), "f")
            .columns(new String[] {"Name", "Type", "Description"})
            .build();

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
        Operator operator = null;
        MutableInt pos = new MutableInt();
        String[] columns = null;
        if (statement.getType() == Type.VARIABLES)
        {
            Map<String, Object> variables = context.getVariables();
            columns = SHOW_VARIABLES_ALIAS.getColumns();
            operator = new Operator()
            {
                @Override
                public RowIterator open(ExecutionContext context)
                {
                    return RowIterator.wrap(variables
                            .entrySet()
                            .stream()
                            .map(e -> (Tuple) Row.of(SHOW_VARIABLES_ALIAS, pos.incrementAndGet(), new Object[] {e.getKey(), e.getValue()}))
                            .iterator());
                }

                @Override
                public int getNodeId()
                {
                    return 0;
                }
            };
        }
        else if (statement.getType() == Type.TABLES)
        {
            String alias = defaultIfBlank(statement.getCatalog(), registry.getDefaultCatalogAlias());
            if (isBlank(alias))
            {
                throw new ParseException("No catalog alias provided.", statement.getToken());
            }
            Catalog catalog = session.getCatalogRegistry().getCatalog(alias);
            if (catalog == null)
            {
                throw new ParseException("No catalog found with alias: " + alias, statement.getToken());
            }

            List<String> tables = catalog.getTables(session, alias);
            columns = SHOW_TABLES_ALIAS.getColumns();
            operator = new Operator()
            {
                @Override
                public RowIterator open(ExecutionContext context)
                {
                    return RowIterator.wrap(tables
                            .stream()
                            .map(table -> (Tuple) Row.of(SHOW_TABLES_ALIAS, pos.incrementAndGet(), new Object[] {table}))
                            .iterator());
                }

                @Override
                public int getNodeId()
                {
                    return 0;
                }
            };
        }
        else if (statement.getType() == Type.FUNCTIONS)
        {
            String alias = defaultIfBlank(statement.getCatalog(), registry.getDefaultCatalogAlias());
            Catalog catalog = session.getCatalogRegistry().getCatalog(alias);
            if (!isBlank(statement.getCatalog()) && catalog == null)
            {
                throw new ParseException("No catalog found with alias: " + statement.getCatalog(), statement.getToken());
            }

            Catalog builtIn = session.getCatalogRegistry().getBuiltin();
            Collection<FunctionInfo> functions = catalog != null ? catalog.getFunctions() : emptyList();
            columns = SHOW_FUNCTIONS_ALIAS.getColumns();
            //CSOFF
            operator = new Operator()
            //CSON
            {
                @Override
                public RowIterator open(ExecutionContext context)
                {
                    return RowIterator.wrap(Stream.concat(
                            functions
                                    .stream()
                                    .sorted((a, b) -> a.getName().compareToIgnoreCase(b.getName()))
                                    .map(function -> (Tuple) Row.of(SHOW_FUNCTIONS_ALIAS, pos.incrementAndGet(), new Object[] {function.getName(), function.getType(), function.getDescription()})),
                            Stream.concat(
                                    functions.size() > 0
                                        ? Stream.of(Row.of(SHOW_FUNCTIONS_ALIAS, pos.incrementAndGet(), new Object[] {"-- Built in --", "", ""}))
                                        : Stream.empty(),
                                    builtIn.getFunctions()
                                            .stream()
                                            .sorted((a, b) -> a.getName().compareToIgnoreCase(b.getName()))
                                            .map(function -> Row.of(SHOW_FUNCTIONS_ALIAS, pos.incrementAndGet(), new Object[] {function.getName(), function.getType(), function.getDescription()}))))
                            .iterator());
                }

                @Override
                public int getNodeId()
                {
                    return 0;
                }
            };
        }

        currentSelect = Pair.of(operator, DescribeUtils.getIndexProjection(asList(columns)));
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

        String[] columns = null;
        if (projection instanceof ObjectProjection)
        {
            columns = ((ObjectProjection) projection).getColumns();
        }
        writer.initResult(columns);

        int rowCount = 0;
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
        }
        else
        {
            writer.startRow();
            context.setTuple(DUMMY_ROW);
            projection.writeValue(writer, context);
            writer.endRow();
            rowCount++;
        }

        context.setVariable(ExecutionContext.ROW_COUNT, rowCount);
        
        currentSelect = null;
    }
}
