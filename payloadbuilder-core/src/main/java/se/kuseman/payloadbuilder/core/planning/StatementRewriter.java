package se.kuseman.payloadbuilder.core.planning;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static se.kuseman.payloadbuilder.core.logicalplan.optimization.LogicalPlanOptimizer.resolveExpression;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.InsertIntoData;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.SelectIntoData;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SelectIntoTempTableSink;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
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
import se.kuseman.payloadbuilder.core.statement.LogicalInsertIntoStatement;
import se.kuseman.payloadbuilder.core.statement.LogicalSelectIntoStatement;
import se.kuseman.payloadbuilder.core.statement.LogicalSelectStatement;
import se.kuseman.payloadbuilder.core.statement.PhysicalStatement;
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
        SetStatement result = new SetStatement(statement.getName(), resolveExpression(context.context, statement.getExpression()), statement.isSystemProperty());
        // Eval and set Set-varibles at this stage to be able to use those in Use-statements etc.
        result.execute(context.context);
        return result;
    }

    @Override
    public Statement visit(UseStatement statement, Context context)
    {
        // Execute the use statement to properly set properties during planning
        statement.execute(context.context);
        return new UseStatement(statement.getQname(), resolveExpression(context.context, statement.getExpression()));
    }

    @Override
    public Statement visit(DescribeSelectStatement statement, Context context)
    {
        context.analyze = statement.isAnalyze();
        PhysicalStatement physicalStatement = (PhysicalStatement) statement.getStatement()
                .accept(this, context);
        context.analyze = false;
        return new PhysicalStatement(new DescribePlan(context.getNextNodeId(), physicalStatement.getPlan(), statement.isAnalyze()));
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

                    TableScan catalogFunctionsScan = new TableScan(TableSchema.EMPTY,
                            new TableSourceReference(0, TableSourceReference.Type.TABLE, "sys", QualifiedName.of(catalog, "functions"), "fCat"),
                            se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection.ALL, emptyList(), null);
                    TableScan systemFunctionsScan = new TableScan(TableSchema.EMPTY, new TableSourceReference(1, TableSourceReference.Type.TABLE, "sys", QualifiedName.of("functions"), "fSys"),
                            se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection.ALL, emptyList(), null);

                    // Order by type and name
                    List<SortItem> sortItems = asList(new SortItem(new LiteralIntegerExpression(2), Order.ASC, NullOrder.UNDEFINED, null),
                            new SortItem(new LiteralIntegerExpression(1), Order.ASC, NullOrder.UNDEFINED, null));

                    //@formatter:off
                    return new LogicalSelectStatement(
                            new Concatenation(
                                    emptyList(),
                                    asList(
                                    new Sort(catalogFunctionsScan, sortItems),
                                    new Projection(ConstantScan.ONE_ROW_EMPTY_SCHEMA,
                                            asList(new LiteralStringExpression("System functions"),
                                                   new LiteralStringExpression(UTF8String.EMPTY),
                                                   new LiteralStringExpression(UTF8String.EMPTY)),
                                            null),
                                    new Sort(systemFunctionsScan, sortItems)), null), false).accept(this, context);
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
        TableSourceReference tableSourceRef = new TableSourceReference(2, TableSourceReference.Type.TABLE, "sys", qname, "t");
        return new LogicalSelectStatement(new TableScan(TableSchema.EMPTY, tableSourceRef, se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection.ALL, emptyList(), null), false).accept(this,
                context);
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
        return createPhysicalPlan(statement, context);
    }

    @Override
    public Statement visit(LogicalSelectIntoStatement statement, Context context)
    {
        return createInsertStatement(statement, context, null);
    }

    @Override
    public Statement visit(LogicalInsertIntoStatement statement, Context context)
    {
        List<String> columns = statement.getColumns();
        return createInsertStatement(statement, context, columns);
    }

    @Override
    public Statement visit(DropTableStatement statement, Context context)
    {
        return statement;
    }

    private Statement createInsertStatement(LogicalSelectIntoStatement statement, Context context, List<String> insertColumns)
    {
        PhysicalStatement input = createPhysicalPlan(statement.getInput(), context);

        List<Option> options = statement.getOptions()
                .stream()
                .map(o -> new Option(o.getOption(), resolveExpression(context.context, o.getValueExpression())))
                .toList();

        Schema schema = input.getPlan()
                .getSchema();
        boolean isAsteriskSchema = SchemaUtils.isAsterisk(schema);
        List<Column> columns = isAsteriskSchema ? emptyList()
                : schema.getColumns();

        // Validate the Insert Into
        if (insertColumns != null
                && !isAsteriskSchema)
        {
            if (insertColumns.size() != columns.size())
            {
                throw new ParseException("Insert column count doesn't match input column count. Insert columns: " + insertColumns + ", input columns: " + columns, statement.getLocation());
            }
        }

        String catalogAlias = defaultIfBlank(statement.getCatalogAlias(), context.getSession()
                .getDefaultCatalogAlias());
        Catalog catalog = context.getSession()
                .getCatalog(catalogAlias);

        int nodeId = context.getNextNodeId();
        //@formatter:off
        IDatasink sink = insertColumns == null
                ? catalog.getSelectIntoSink(context.getSession(), catalogAlias, statement.getTable(), new SelectIntoData(nodeId, columns, options))
                : catalog.getInsertIntoSink(context.getSession(), catalogAlias, statement.getTable(), new InsertIntoData(nodeId, columns, options, insertColumns));
        //@formatter:on

        // If this is a temporary table select into then we need to extract table schema + indices into context to let the rest of the query
        // know about them
        if (Catalog.SYSTEM_CATALOG_ALIAS.equalsIgnoreCase(statement.getCatalogAlias())
                && insertColumns == null // -> SelectInto
                && "#".equals(statement.getTable()
                        .getFirst()))
        {
            // We need to extract the generated indices from the sink and hence need this ugly cast
            if (!(sink instanceof SelectIntoTempTableSink))
            {
                throw new IllegalArgumentException("Expected temporary table sink to be of type: " + SelectIntoTempTableSink.class.getName());
            }

            List<Index> indices = ((SelectIntoTempTableSink) sink).getIndices();
            TableSchema tableSchema = new TableSchema(context.currentLogicalPlan.getSchema(), indices);
            context.context.getSession()
                    .setTemporaryTableSchema(statement.getTable()
                            .extract(1)
                            .toLowerCase(), tableSchema);
        }

        return new PhysicalStatement(QueryPlanner.wrapWithAnalyze(context, new InsertInto(nodeId, input.getPlan(), insertColumns, sink)));
    }

    private PhysicalStatement createPhysicalPlan(LogicalSelectStatement statement, Context context)
    {
        ILogicalPlan plan = statement.getSelect();
        QuerySession s = context.getSession();

        ValueVector printPlanProperty = s.getSystemProperty(QuerySession.PRINT_PLAN);
        boolean printPlan = !printPlanProperty.isNull(0)
                && printPlanProperty.getBoolean(0);

        // Optimize plan
        plan = LogicalPlanOptimizer.optimize(context.context, plan, context.schemaByTableSource);

        if (printPlan)
        {
            s.printLine("Logical plan: " + System.lineSeparator());
            s.printLine(plan.print(0));
        }

        context.currentLogicalPlan = plan;

        // Reset counter for each statement
        context.nodeIdCounter = 0;
        IPhysicalPlan physicalPlan = plan.accept(QUERY_PLANNER, context);

        // Wrap plan to suppress writing to output
        if (statement.isAssignmentSelect())
        {
            physicalPlan = new AssignmentPlan(context.getNextNodeId(), physicalPlan);
        }

        if (printPlan)
        {
            s.printLine("Physical plan: " + System.lineSeparator());
            s.printLine(physicalPlan.print(0));
        }

        // Create a physical plan from the logical
        return new PhysicalStatement(physicalPlan);
    }
}
