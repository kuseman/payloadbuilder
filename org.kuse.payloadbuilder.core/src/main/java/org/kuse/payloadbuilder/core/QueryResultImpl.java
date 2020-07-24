package org.kuse.payloadbuilder.core;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_OBJECT_ARRAY;
import static org.apache.commons.lang3.StringUtils.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.ObjectProjection;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.OperatorBuilder;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.parser.DescribeFunctionStatement;
import org.kuse.payloadbuilder.core.parser.DescribeSelectStatement;
import org.kuse.payloadbuilder.core.parser.DescribeTableStatement;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.IfStatement;
import org.kuse.payloadbuilder.core.parser.PrintStatement;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QueryStatement;
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
    private static final Row DUMMY_ROW = Row.of(TableAlias.of(null, "dummy", "d"), 0, EMPTY_OBJECT_ARRAY);
    private static final TableAlias SHOW_PARAMETERS_ALIAS = new TableAlias(null, QualifiedName.of("parameters"), "p", new String[] {"Name", "Value"});
    private static final TableAlias SHOW_VARIABLES_ALIAS = new TableAlias(null, QualifiedName.of("variables"), "v", new String[] {"Name", "Value"});

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
        Object value = statement.getExpression().eval(context);
        session.printLine(value);
        return null;
    }

    @Override
    public Void visit(IfStatement statement, Void ctx)
    {
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
            context.getSession().setDefaultCatalog(statement.getQname().getFirst());
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
        currentSelect = DescribeUtils.getDescribeSelect(session, statement.getSelectStatement().getSelect()); //Pair.of(describeOperator, describeProjection);
        return null;
    }

    @Override
    public Void visit(DescribeFunctionStatement statement, Void ctx)
    {
        throw new RuntimeException("IMPLEMENT");
    }

    @Override
    public Void visit(ShowStatement statement, Void ctx)
    {
        context.clear();
        Operator operator = null;
        MutableInt pos = new MutableInt();
        String[] columns = null;
        if (statement.getType() == Type.PARAMETERS)
        {
            Map<String, Object> parameters = context.getSession().getParameters();
            columns = SHOW_PARAMETERS_ALIAS.getColumns();
            operator = new Operator()
            {
                @Override
                public Iterator<Row> open(ExecutionContext context)
                {
                    return parameters
                            .entrySet()
                            .stream()
                            .map(e -> Row.of(SHOW_PARAMETERS_ALIAS, pos.incrementAndGet(), new Object[] {e.getKey(), e.getValue()}))
                            .iterator();
                }

                @Override
                public int getNodeId()
                {
                    return 0;
                }
            };
        }
        else
        {
            Map<String, Object> variables = context.getVariables();
            columns = SHOW_VARIABLES_ALIAS.getColumns();
            operator = new Operator()
            {
                @Override
                public Iterator<Row> open(ExecutionContext context)
                {
                    return variables
                            .entrySet()
                            .stream()
                            .map(e -> Row.of(SHOW_VARIABLES_ALIAS, pos.incrementAndGet(), new Object[] {e.getKey(), e.getValue()}))
                            .iterator();
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
        //        context.clearStatementCache();
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
        
        context.clear();
        if (operator != null)
        {
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

        //        ObjectWriter printer = new ObjectMapper().writerWithDefaultPrettyPrinter();
        //        context.getOperatorContext().getNodeData().forEachEntry((i, e) ->
        //        {
        //            try
        //            {
        //                System.out.println("Node: " + i);
        //                System.out.println(printer.writeValueAsString(e));
        //            }
        //            catch (JsonProcessingException ee)
        //            {
        //            }
        //            return true;
        //        });

        currentSelect = null;
        //        System.out.println(QualifiedReferenceExpression.executionsByName);
        //        QualifiedReferenceExpression.executionsByName.clear();
    }
}
