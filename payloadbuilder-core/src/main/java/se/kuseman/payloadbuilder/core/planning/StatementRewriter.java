package se.kuseman.payloadbuilder.core.planning;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static se.kuseman.payloadbuilder.core.logicalplan.optimization.LogicalPlanOptimizer.resolveExpression;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.common.Option;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Concatenation;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.logicalplan.optimization.LogicalPlanOptimizer;
import se.kuseman.payloadbuilder.core.parser.ParseException;
import se.kuseman.payloadbuilder.core.physicalplan.AssignmentPlan;
import se.kuseman.payloadbuilder.core.physicalplan.DescribePlan;
import se.kuseman.payloadbuilder.core.physicalplan.IPhysicalPlan;
import se.kuseman.payloadbuilder.core.physicalplan.InsertInto;
import se.kuseman.payloadbuilder.core.planning.StatementPlanner.Context;
import se.kuseman.payloadbuilder.core.statement.CacheFlushRemoveStatement;
import se.kuseman.payloadbuilder.core.statement.DescribeSelectStatement;
import se.kuseman.payloadbuilder.core.statement.DropTableStatement;
import se.kuseman.payloadbuilder.core.statement.IfStatement;
import se.kuseman.payloadbuilder.core.statement.InsertIntoStatement;
import se.kuseman.payloadbuilder.core.statement.LogicalSelectStatement;
import se.kuseman.payloadbuilder.core.statement.PhysicalSelectStatement;
import se.kuseman.payloadbuilder.core.statement.PrintStatement;
import se.kuseman.payloadbuilder.core.statement.SetStatement;
import se.kuseman.payloadbuilder.core.statement.ShowStatement;
import se.kuseman.payloadbuilder.core.statement.Statement;
import se.kuseman.payloadbuilder.core.statement.StatementList;
import se.kuseman.payloadbuilder.core.statement.StatementVisitor;
import se.kuseman.payloadbuilder.core.statement.UseStatement;

/** Statement visitor that rewrites a {@link Statement} and creates a physical plan */
class StatementRewriter implements StatementVisitor<Statement, StatementPlanner.Context>
{
    private static final QueryPlanner QUERY_PLANNER = new QueryPlanner();

    @Override
    public Statement visit(IfStatement statement, Context context)
    {
        List<Statement> statements = statement.getStatements()
                .stream()
                .map(s -> s.accept(this, context))
                .collect(toList());
        List<Statement> elseStatements = statement.getElseStatements()
                .stream()
                .map(s -> s.accept(this, context))
                .collect(toList());

        IExpression condition = resolveExpression(context.context, statement.getCondition());

        // Fold if statement
        if (condition.isConstant())
        {
            boolean result = condition.eval(TupleVector.CONSTANT, context.context)
                    .getPredicateBoolean(0);
            return new StatementList(result ? statements
                    : elseStatements);
        }

        return new IfStatement(condition, statements, elseStatements);
    }

    @Override
    public Statement visit(PrintStatement statement, Context context)
    {
        return new PrintStatement(resolveExpression(context.context, statement.getExpression()));
    }

    @Override
    public Statement visit(SetStatement statement, Context context)
    {
        return new SetStatement(statement.getName(), resolveExpression(context.context, statement.getExpression()), statement.isSystemProperty());
    }

    @Override
    public Statement visit(UseStatement statement, Context context)
    {
        // Change of default catalog that would be done runtime so that planning gets correct
        if (statement.getExpression() == null)
        {
            context.getSession()
                    .setDefaultCatalogAlias(statement.getQname()
                            .getFirst());
        }

        return new UseStatement(statement.getQname(), resolveExpression(context.context, statement.getExpression()));
    }

    @Override
    public Statement visit(DescribeSelectStatement statement, Context context)
    {
        context.analyze = statement.isAnalyze();
        PhysicalSelectStatement physicalSelectStatement = (PhysicalSelectStatement) statement.getSelectStatement()
                .accept(this, context);
        context.analyze = false;

        PhysicalSelectStatement physicalDescribeStatement = new PhysicalSelectStatement(new DescribePlan(context.getNextNodeId(), physicalSelectStatement.getSelect(), statement.isAnalyze()));

        if (statement.isIncludeLogicalPlan())
        {
            // ILogicalPlan logicalPlan = context.currentLogicalPlan;
            // return new StatementList(asList( ))
        }

        return physicalDescribeStatement;
    }

    @Override
    public Statement visit(ShowStatement statement, Context context)
    {
        // Construct selects from system catalog
        String tableName = null;
        String catalog = statement.getCatalog();

        switch (statement.getType())
        {
            case CACHES:
                tableName = "caches";
                // Catalogs cannot have caches
                catalog = null;
                break;
            case FUNCTIONS:
                // No catalog then show both default catalog functions plus system functions
                if (isBlank(catalog))
                {
                    catalog = context.context.getSession()
                            .getDefaultCatalogAlias();

                    TableScan catalogFunctionsScan = new TableScan(TableSchema.EMPTY, new TableSourceReference("sys", QualifiedName.of(catalog, "functions"), "fCat"), emptyList(), false, emptyList(),
                            null);
                    TableScan systemFunctionsScan = new TableScan(TableSchema.EMPTY, new TableSourceReference("sys", QualifiedName.of("functions"), "fSys"), emptyList(), false, emptyList(), null);

                    // Order by type and name
                    List<SortItem> sortItems = asList(new SortItem(new LiteralIntegerExpression(2), Order.ASC, NullOrder.UNDEFINED, null),
                            new SortItem(new LiteralIntegerExpression(1), Order.ASC, NullOrder.UNDEFINED, null));

                    //@formatter:off
                    return new LogicalSelectStatement(
                            new Concatenation(asList(
                                    new Sort(catalogFunctionsScan, sortItems),
                                    new Projection(ConstantScan.INSTANCE,
                                            asList(new LiteralStringExpression("System functions"),
                                                   new LiteralStringExpression(UTF8String.EMPTY),
                                                   new LiteralStringExpression(UTF8String.EMPTY)),
                                            false),
                                    new Sort(systemFunctionsScan, sortItems))), false).accept(this, context);
                    //@formatter:on

                }
                tableName = "functions";
                break;
            case TABLES:
                tableName = "tables";
                break;
            case VARIABLES:
                tableName = "variables";
                break;
            default:
                throw new IllegalArgumentException("Unsupported SHOW type " + statement.getType());
        }

        QualifiedName qname = isBlank(catalog) ? QualifiedName.of(tableName)
                : QualifiedName.of(catalog, tableName);
        TableSourceReference tableSourceRef = new TableSourceReference("sys", qname, "t");
        return new LogicalSelectStatement(new TableScan(TableSchema.EMPTY, tableSourceRef, emptyList(), false, emptyList(), null), false).accept(this, context);
    }

    @Override
    public Statement visit(CacheFlushRemoveStatement statement, Context context)
    {
        return new CacheFlushRemoveStatement(statement.getType(), statement.getName(), statement.isAll(), statement.isFlush(), resolveExpression(context.context, statement.getKey()));
    }

    @Override
    public Statement visit(StatementList statement, Context context)
    {
        return new StatementList(statement.getStatements()
                .stream()
                .map(s -> s.accept(this, context))
                .collect(toList()));
    }

    @Override
    public Statement visit(LogicalSelectStatement statement, Context context)
    {
        ILogicalPlan plan = statement.getSelect();

        // Optimize plan
        plan = LogicalPlanOptimizer.optimize(context.context, plan, context.schemaByTempTable);

        context.currentLogicalPlan = plan;

        // System.out.println(plan.print(0));

        IPhysicalPlan physicalPlan = plan.accept(QUERY_PLANNER, context);

        // Wrap plan to suppress writing to output
        if (statement.isAssignmentSelect())
        {
            physicalPlan = new AssignmentPlan(context.getNextNodeId(), physicalPlan);
        }

        // Create a physical plan from the logical
        return new PhysicalSelectStatement(physicalPlan);
    }

    @Override
    public Statement visit(PhysicalSelectStatement statement, Context context)
    {
        throw new IllegalArgumentException("A physical select statement should not be visited");
    }

    @Override
    public Statement visit(InsertIntoStatement statement, Context context)
    {
        // Visit select statement to retrieve the schema for the temp table
        PhysicalSelectStatement selectStatement = (PhysicalSelectStatement) statement.getSelectStatement()
                .accept(this, context);
        context.schemaByTempTable.put(statement.getTable()
                .toLowerCase(), context.currentLogicalPlan.getSchema());

        List<Option> options = statement.getOptions()
                .stream()
                .map(o -> new Option(o.getOption(), resolveExpression(context.context, o.getValueExpression())))
                .collect(toList());

        return new PhysicalSelectStatement(new InsertInto(context.getNextNodeId(), selectStatement.getSelect(), statement.getTable(), options));
    }

    @Override
    public Statement visit(DropTableStatement statement, Context context)
    {
        if (!statement.isTempTable())
        {
            throw new ParseException("DROP TABLE can only be performed on temporary tables", statement.getToken());
        }
        return statement;
    }
}
