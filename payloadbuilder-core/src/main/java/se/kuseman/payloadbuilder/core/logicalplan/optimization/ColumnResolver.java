package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.equalsAnyIgnoreCase;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo.SchemaResolveException;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IDereferenceExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference.ColumnReference;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedFunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedSubQueryExpression;
import se.kuseman.payloadbuilder.core.expression.VariableExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.SubQuery;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.parser.ParseException;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * Rule to optimize columns and eliminate sub queries.
 * 
 * <pre>
 * This rule binds column expressions to the input, taking care of
 * asterisk table sources etc. Correrlated sub queries and so on.
 * 
 * </pre>
 */
class ColumnResolver extends ALogicalPlanOptimizer<ColumnResolver.Ctx>
{
    ColumnResolver()
    {
        // We handle all expression rewriting when handling the logical plans
        super(null);
    }

    /** Visitor context */
    static class Ctx extends ALogicalPlanOptimizer.Context
    {
        final ColumnResolver columnResvoler;

        /** Schema used when resolving expressions in {@link ColumnResolverVisitor} */
        ResolveSchema schema;

        /** Concated outer schema used by joins/sub query expressions to detect outer column references in {@link ColumnResolverVisitor} */
        ResolveSchema outerSchema;

        /** Outer references found during traversal. Used to detect correlated sub queries. Added in {@link ColumnResolverVisitor} */
        Set<Column> outerReferences;

        /** Stack with the last visited plans resulting schema */
        Deque<ResolveSchema> planSchema = new ArrayDeque<>();

        /** Map with type for each lambda id */
        Int2ObjectMap<ResolvedType> lambdaTypes = new Int2ObjectOpenHashMap<>();

        /** Flag to indicate when we are resolving inside a sub query */
        boolean insideSubQuery = false;

        /**
         * Set with column references used when resolving aggregate projection to detect if an expression should be single value or grouped
         */
        Map<TableSourceReference, Set<String>> aggregateColumnReferences;

        Ctx(IExecutionContext context, ColumnResolver columnResolver)
        {
            super(context);
            this.columnResvoler = columnResolver;
        }

        /**
         * Return the schema for provided plan, either the top of the plan schema stack or the input plan if column resolver doesn't process the logical plan type
         */
        ResolveSchema getSchema(ILogicalPlan plan)
        {
            if (planSchema.size() > 0)
            {
                return planSchema.pop();
            }

            return new ResolveSchema(plan.getSchema());
        }
    }

    @Override
    protected Ctx createContext(IExecutionContext context)
    {
        return new Ctx(context, this);
    }

    @Override
    ILogicalPlan optimize(Context context, ILogicalPlan plan)
    {
        return plan.accept(this, (Ctx) context);
    }

    @Override
    public ILogicalPlan visit(Projection plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        // Rewrite projections according to input plan schema
        ResolveSchema schema = context.getSchema(input);
        context.schema = schema;
        List<IExpression> expressions = ColumnResolverVisitor.rewrite(context, plan.getExpressions());

        // Expand all static asterisks, they should not be left for execution
        expressions = expandAsterisks(expressions, context.outerReferences, context.outerSchema, schema);

        // Might create the resulting schema from the expressions here and not do that on demand
        ILogicalPlan result = new Projection(input, expressions);
        context.planSchema.push(new ResolveSchema(result.getSchema()));
        return result;
    }

    @Override
    public ILogicalPlan visit(Filter plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        // Rewrite predicate according to input plan schema
        ResolveSchema schema = context.getSchema(input);
        context.schema = schema;
        IExpression predicate = ColumnResolverVisitor.rewrite(context, plan.getPredicate());

        ILogicalPlan result = new Filter(input, plan.getTableSource(), predicate);

        context.planSchema.push(schema);
        return result;
    }

    @Override
    public ILogicalPlan visit(Sort plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);
        ResolveSchema schema = context.getSchema(input);

        if (input instanceof Sort)
        {
            // TODO: in populated join then this is valid
            throw new ParseException("Sort is invalid in sub queries", Location.EMPTY);
        }

        // Rewrite sort items expressions
        context.schema = schema;
        List<SortItem> sortItems = plan.getSortItems()
                .stream()
                .map(si -> new SortItem(ColumnResolverVisitor.rewrite(context, si.getExpression()), si.getOrder(), si.getNullOrder(), si.getLocation()))
                .collect(toList());

        ILogicalPlan result = new Sort(input, sortItems);
        context.planSchema.push(schema);
        return result;
    }

    @Override
    public ILogicalPlan visit(Join plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan left = plan.getOuter()
                .accept(this, context);

        // CSOFF
        // Concat outer and left schema to detect outer references
        ResolveSchema prevOuterSchema = context.outerSchema;
        // Reset the outer reference list before visiting left to distinctly
        // know what references this join has
        Set<Column> prevOuterReferences = context.outerReferences;
        // CSON

        ResolveSchema leftSchema = context.getSchema(left);

        context.outerReferences = new HashSet<>();
        // Outer reference are only supported in outer/cross apply's so append the outer schema
        if (plan.getCondition() == null
                && plan.getType() != Join.Type.CROSS)
        {
            context.outerSchema = ResolveSchema.concat(context.outerSchema, leftSchema);
        }

        // CSOFF
        Schema outerSchema = context.outerSchema != null ? context.outerSchema.getSchema()
                : Schema.EMPTY;
        // CSON

        ILogicalPlan right = plan.getInner()
                .accept(this, context);

        ResolveSchema rightSchema = context.getSchema(right);

        // Construct a resulting join schema
        ResolveSchema resultSchema = new ResolveSchema();
        resultSchema.schemas.addAll(leftSchema.schemas);
        if (plan.getPopulateAlias() != null)
        {
            Schema populatedSchema = Schema.of(SchemaUtils.getPopulatedColumn(plan.getPopulateAlias(), rightSchema.getSchema()));
            resultSchema.add(new AliasSchema(populatedSchema, plan.getPopulateAlias()), null);
        }
        else
        {
            resultSchema.addAll(rightSchema.schemas, null);
        }

        IExpression condition = plan.getCondition();

        // Rewrite condition if present. Cross/outer joins doesn't have conditions
        if (condition != null)
        {
            context.schema = ResolveSchema.concat(leftSchema, rightSchema);
            condition = ColumnResolverVisitor.rewrite(context, condition);
        }

        // CSOFF
        Join result = new Join(left, right, plan.getType(), plan.getPopulateAlias(), condition, context.outerReferences, plan.isSwitchedInputs(), outerSchema);
        // CSON

        // Restore context values
        context.outerSchema = prevOuterSchema;
        // Add all new outer references to previous list to cascade the list upwards
        if (prevOuterReferences != null
                && context.outerReferences != null)
        {
            prevOuterReferences.addAll(context.outerReferences);
        }
        context.outerReferences = prevOuterReferences;

        context.planSchema.push(resultSchema);

        return result;
    }

    @Override
    public ILogicalPlan visit(Aggregate plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        ResolveSchema schema = context.getSchema(input);
        // Resolve expressions against input schema
        context.schema = schema;

        List<IExpression> aggregateExpressions = ColumnResolverVisitor.rewrite(context, plan.getAggregateExpressions());
        context.aggregateColumnReferences = collectColumns(aggregateExpressions);

        List<IAggregateExpression> projectionExpressions = new ArrayList<>();
        int size = plan.getProjectionExpressions()
                .size();
        for (int i = 0; i < size; i++)
        {
            IExpression exp = ColumnResolverVisitor.rewrite(context, plan.getProjectionExpressions()
                    .get(i));

            if (exp instanceof AggregateWrapperExpression awe
                    && awe.getExpression() instanceof AsteriskExpression)
            {
                List<IExpression> expandAsterisks = expandAsterisks(singletonList(awe.getExpression()), context.outerReferences, context.outerSchema, schema);
                for (IExpression ea : expandAsterisks)
                {
                    projectionExpressions.add(new AggregateWrapperExpression(ea, awe.isSingleValue(), false));
                }
            }
            else
            {
                projectionExpressions.add((IAggregateExpression) exp);
            }
        }

        ILogicalPlan result = new Aggregate(input, aggregateExpressions, projectionExpressions);
        context.planSchema.push(new ResolveSchema(result.getSchema()));
        return result;
    }

    @Override
    public ILogicalPlan visit(SubQuery plan, Ctx context)
    {
        // CSOFF
        boolean prevInsideSubQuery = context.insideSubQuery;
        // CSON
        context.insideSubQuery = true;

        // Eliminate sub query, no sub queries should be left
        ILogicalPlan result = plan.getInput()
                .accept(this, context);

        ResolveSchema schema = context.getSchema(result);

        // Validate sub query schema, all columns must have a specified alias
        int size = schema.getSize();
        for (int i = 0; i < size; i++)
        {
            Column column = schema.getColumn(i)
                    .getValue();
            boolean isAsterisk = SchemaUtils.isAsterisk(column, false);
            if ("".equals(column.getName())
                    && !isAsterisk)
            {
                throw new ParseException("Missing column name for ordinal " + i + " of " + plan.getAlias(), plan.getLocation());
            }
        }
        context.planSchema.push(new ResolveSchema(schema, plan.getTableSource()));
        context.insideSubQuery = prevInsideSubQuery;
        return new SubQuery(result, plan.getTableSource(), plan.getLocation());
    }

    @Override
    public ILogicalPlan visit(TableFunctionScan plan, Ctx context)
    {
        // CSOFF
        ResolveSchema prevSchema = context.schema;
        context.schema = null;
        // Table function only have outer schema to search values from
        List<IExpression> expressions = ColumnResolverVisitor.rewrite(context, plan.getArguments());
        // CSON

        ResolveSchema prevOuterSchema = context.outerSchema;
        context.outerSchema = null;

        context.schema = prevSchema;
        context.outerSchema = prevOuterSchema;

        TableFunctionInfo function = context.getSession()
                .resolveTableFunctionInfo(plan.getTableSource()
                        .getCatalogAlias(),
                        plan.getTableSource()
                                .getName()
                                .getFirst())
                .getValue();

        // Resolve the table function schema from it's resolved arguments
        Schema schema = Schema.EMPTY;
        TableSourceReference tableSource = plan.getTableSource();
        try
        {
            schema = function.getSchema(expressions, plan.getOptions());
        }
        catch (SchemaResolveException e)
        {
            throw new ParseException(e.getMessage(), plan.getLocation());
        }

        if (SchemaUtils.isAsterisk(schema, true))
        {
            schema = Schema.of(new CoreColumn(plan.getAlias(), ResolvedType.of(Type.Any), "", false, tableSource, CoreColumn.Type.ASTERISK));
        }
        else
        {
            // .. force all columns to have a proper column reference
            schema = SchemaResolver.recreate(tableSource, schema);
        }

        context.planSchema.push(new ResolveSchema(schema, plan.getAlias()));

        List<Option> options = plan.getOptions()
                .stream()
                .map(o -> new Option(o.getOption(), ColumnResolverVisitor.rewrite(context, o.getValueExpression())))
                .toList();

        return new TableFunctionScan(tableSource, schema, expressions, options, plan.getLocation());
    }

    @Override
    public ILogicalPlan visit(TableScan plan, Ctx context)
    {
        List<Option> options = plan.getOptions()
                .stream()
                .map(o -> new Option(o.getOption(), ColumnResolverVisitor.rewrite(context, o.getValueExpression())))
                .toList();

        context.planSchema.push(new ResolveSchema(plan.getSchema(), plan.getAlias()));

        return new TableScan(plan.getTableSchema(), plan.getTableSource(), plan.getProjection(), plan.isTempTable(), options, plan.getLocation());
    }

    @Override
    public ILogicalPlan visit(ExpressionScan plan, Ctx context)
    {
        // CSOFF
        ResolveSchema prevSchema = context.schema;
        context.schema = null;
        // Expression scans only have outer schema to search values from
        IExpression expression = ColumnResolverVisitor.rewrite(context, plan.getExpression());
        // CSON

        ResolvedType type = expression.getType();
        // Parse error, expression has invalid type
        if (!(type.getType() == Type.Any
                || type.getType() == Type.Table))
        {
            throw new ParseException("Expression scans must reference either " + Type.Any + " or " + Type.Table + " types.", plan.getLocation());
        }

        TableSourceReference tableSource = plan.getTableSource();
        Schema schema = type.getSchema();
        if (schema == null)
        {
            // If this is a table variable (@table) see if there is an empty vector with the schema
            // in context and use that schema if present
            // TODO: add compiler info message if missing
            if (expression instanceof VariableExpression ve)
            {
                ValueVector value = ((ExecutionContext) context.context).getVariableValue(ve.getName());
                Object obj = value != null ? value.valueAsObject(0)
                        : null;

                if (obj != null
                        && !(obj instanceof TupleVector))
                {
                    throw new ParseException("Table variable expression must be of type: " + Type.Table + " but got: " + value.type(), plan.getLocation());
                }
                else if (obj instanceof TupleVector tv)
                {
                    schema = tv.getSchema();
                }
            }

            if (schema == null)
            {
                schema = Schema.EMPTY;
            }
        }

        if (SchemaUtils.isAsterisk(schema, true))
        {
            // Set an asterisk schema with expression scans table source
            schema = Schema.of(new CoreColumn(plan.getAlias(), ResolvedType.of(Type.Any), "", false, tableSource, CoreColumn.Type.ASTERISK));
        }
        else
        {
            // .. else force all columns to have a proper column reference
            schema = SchemaResolver.recreate(tableSource, schema);
        }

        context.schema = prevSchema;

        context.planSchema.push(new ResolveSchema(schema, plan.getAlias()));

        return new ExpressionScan(tableSource, schema, expression, plan.getLocation());
    }

    @Override
    public ILogicalPlan visit(OperatorFunctionScan plan, Ctx context)
    {
        Pair<String, OperatorFunctionInfo> pair = context.getSession()
                .resolveOperatorFunctionInfo(plan.getCatalogAlias(), plan.getFunction());
        if (pair == null)
        {
            throw new ParseException("No operator function found named: " + plan.getFunction(), plan.getLocation());
        }
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        Column column = plan.getSchema()
                .getColumns()
                .get(0);

        boolean isInternal = SchemaUtils.isInternal(column);

        // Recreate the operator schema
        Schema schema = Schema.of(new CoreColumn(column.getName(), pair.getValue()
                .getType(input.getSchema()), "", isInternal, null, CoreColumn.Type.REGULAR));

        context.planSchema.push(new ResolveSchema(schema));
        return new OperatorFunctionScan(schema, input, plan.getCatalogAlias(), plan.getFunction(), plan.getLocation());
    }

    private List<IExpression> expandAsterisks(List<IExpression> expressions, Set<Column> outerReferences, ResolveSchema outerSchema, ResolveSchema inputSchema)
    {
        boolean schemaHasAsterisks = inputSchema.hasAsteriskColumns();

        List<IExpression> result = new ArrayList<>();
        for (IExpression expression : expressions)
        {
            if (expression instanceof AsteriskExpression)
            {
                AsteriskExpression ae = (AsteriskExpression) expression;
                QualifiedName qname = ae.getQname();

                int startIndex = 0;
                int populateIndex = -1;
                TableSourceReference populatedTableRef = null;
                boolean useTableRef = true;
                String populateAlias = null;
                ResolveSchema schema = null;
                boolean outerReference = false;

                // Full schema
                if (qname.size() == 0)
                {
                    schema = inputSchema;
                }
                // Qualified schema
                else if (qname.size() == 1)
                {
                    String alias = qname.getFirst()
                            .toLowerCase();

                    Pair<Integer, ResolveSchema> pair = inputSchema.getResolveSchema(alias);

                    if (pair == null)
                    {
                        // Try outer
                        pair = outerSchema != null ? outerSchema.getResolveSchema(alias)
                                : null;
                        schemaHasAsterisks = outerSchema.hasAsteriskColumns();
                        outerReference = true;
                        startIndex = pair.getKey();
                        schema = pair.getValue();
                    }
                    else
                    {
                        startIndex = pair.getKey();
                        schema = pair.getValue();
                    }
                }
                // Expand nested value like map, tuple vector etc.
                else if (qname.size() > 1)
                {
                    throw new ParseException("Asterisk for nested values is not supported yet", ae.getLocation());
                }

                // Asterisk schemas are expanded runtime
                if (SchemaUtils.isAsterisk(schema.getSchema(), false))
                {
                    result.add(ae);
                    continue;
                }

                int size = schema.getSize();
                // Populated, dig into populate schema. We only do this when we target an alias
                if (schema.isPopulated()
                        && qname.size() > 0)
                {
                    if (outerReference)
                    {
                        outerReferences.add(schema.getColumn(0)
                                .getValue());
                    }
                    populateIndex = startIndex;
                    populateAlias = qname.getFirst()
                            .toLowerCase();

                    Schema populatedSchema = schema.getColumn(0)
                            .getValue()
                            .getType()
                            .getSchema();

                    schema = new ResolveSchema(populatedSchema);

                    // Fetch table source ref from the populated schema
                    // this will be used on all columns further down
                    // to properly make ColumnExpression#eval work
                    populatedTableRef = SchemaUtils.getTableSource(populatedSchema);
                    useTableRef = populatedTableRef != null;
                    size = schema.getSize();
                }

                for (int i = 0; i < size; i++)
                {
                    Pair<String, Column> pair = schema.getColumn(i);

                    Column column = pair.getValue();

                    // Internal columns should not be expanded
                    if (SchemaUtils.isInternal(column))
                    {
                        continue;
                    }

                    TableSourceReference columnTableRef = null;
                    if (useTableRef)
                    {
                        columnTableRef = populatedTableRef != null ? populatedTableRef
                                : SchemaUtils.getTableSource(column);
                    }

                    int index;
                    QualifiedName path;

                    if (populateAlias != null)
                    {
                        index = schemaHasAsterisks ? -1
                                : populateIndex;

                        path = QualifiedName.of(populateAlias, column.getName());
                    }
                    else
                    {
                        index = schemaHasAsterisks ? -1
                                : startIndex + i;

                        path = QualifiedName.of(column.getName());
                    }

                    String expressionAlias = column.getName();
                    ResolvedType expressionType = column.getType();
                    // Correct alias etc. when we have a populated hit
                    if (populateAlias != null)
                    {
                        expressionAlias = populateAlias;
                        expressionType = ResolvedType.table(schema.getSchema());
                    }

                    ColumnExpression.Builder builder = ColumnExpression.Builder.of(expressionAlias, expressionType)
                            .withOuterReference(outerReference)
                            .withOrdinal(index);

                    if (columnTableRef != null)
                    {
                        CoreColumn.Type columnType = SchemaUtils.getColumnType(column);
                        builder.withColumnReference(new ColumnReference(columnTableRef, columnType));
                    }
                    // If we don't have an ordinal we use the first part of path as column
                    if (index < 0
                            && path.size() > 0)
                    {
                        builder.withColumn(path.getFirst());
                    }

                    // Strip away column name
                    path = path.extract(1);

                    if (path.size() > 0)
                    {
                        result.add(DereferenceExpression.create(builder.build(), path, ae.getLocation()));
                    }
                    else
                    {
                        result.add(builder.build());
                    }
                }
            }
            else
            {
                result.add(expression);
            }
        }
        return result;
    }

    static class ColumnResolverVisitor extends ARewriteExpressionVisitor<ColumnResolver.Ctx>
    {
        private static final ColumnResolverVisitor INSTANCE = new ColumnResolverVisitor();

        /** Rewrite provided expression according to provided schema */
        static IExpression rewrite(ColumnResolver.Ctx context, IExpression expression)
        {
            return expression.accept(INSTANCE, context);
        }

        /** Rewrite provided expressions according to provided schema */
        static List<IExpression> rewrite(ColumnResolver.Ctx context, List<IExpression> expressions)
        {
            ResolveSchema schema = context.schema;
            ResolveSchema outerSchema = context.outerSchema;

            List<IExpression> result = new ArrayList<>(expressions.size());
            for (IExpression expression : expressions)
            {
                result.add(expression.accept(INSTANCE, context));

                // Restore context between resolves since there could be sub query expressions that sets these
                context.schema = schema;
                context.outerSchema = outerSchema;
            }
            return result;
        }

        @Override
        public IExpression visit(AggregateWrapperExpression expression, Ctx context)
        {
            IExpression result = expression.getExpression()
                    .accept(this, context);

            /*
             * If this is a wrapper around an asterisk expression then simply return the input. This will be expanded into column expressions later on and the singleValue property on the wrapper
             * should be not be altered with because it's used when creating the expanded expressions
             */
            if (result instanceof AsteriskExpression)
            {
                return new AggregateWrapperExpression(result, expression.isSingleValue(), expression.isInternal());
            }

            Map<TableSourceReference, Set<String>> columns = ColumnResolver.collectColumns(singletonList(result));
            boolean singleValue;

            if (!columns.isEmpty())
            {
                singleValue = true;
                for (Entry<TableSourceReference, Set<String>> e : columns.entrySet())
                {
                    Set<String> aggregateColumns = context.aggregateColumnReferences.get(e.getKey());

                    if (aggregateColumns == null
                            || !CollectionUtils.containsAll(aggregateColumns, e.getValue()))
                    {
                        singleValue = false;
                        break;
                    }
                }
            }
            else
            {
                singleValue = true;
            }

            return new AggregateWrapperExpression(result, singleValue, expression.isInternal());
        }

        @Override
        public IExpression visit(IDereferenceExpression expression, Ctx context)
        {
            return DereferenceExpression.create(expression.getExpression()
                    .accept(this, context), QualifiedName.of(expression.getRight()), null);
        }

        @Override
        public IExpression visit(IFunctionCallExpression expression, Ctx context)
        {
            if (expression.getFunctionInfo() instanceof LambdaFunction)
            {
                int size = expression.getArguments()
                        .size();
                List<IExpression> arguments = new ArrayList<>(Collections.nCopies(size, null));

                LambdaFunction lf = (LambdaFunction) expression.getFunctionInfo();

                // For each lambda binding, resolve the schema for each lambda index, will
                // be used later on when resolving column references
                for (LambdaFunction.LambdaBinding lb : lf.getLambdaBindings())
                {
                    int lambdaArg = lb.getLambdaArg();
                    int toArg = lb.getToArg();

                    IExpression toArgExpression = expression.getArguments()
                            .get(toArg)
                            .accept(this, context);

                    // Set the resolved toArg expression to result
                    arguments.set(toArg, toArgExpression);

                    // See if the resolved type is known, else we will have an empty schema
                    // for the lambda alias and all references will be Dereferecnes that evals
                    // runtime.
                    ResolvedType toArgType = toArgExpression.getType();

                    LambdaExpression le = (LambdaExpression) expression.getArguments()
                            .get(lambdaArg);

                    // Set lambda types for all lambda ids
                    for (int lambdaId : le.getLambdaIds())
                    {
                        context.lambdaTypes.put(lambdaId, toArgType);
                    }

                    // Resolve the lambda's expression
                    arguments.set(lambdaArg, new LambdaExpression(le.getIdentifiers(), le.getExpression()
                            .accept(this, context), le.getLambdaIds()));

                    // Clean up
                    for (int lambdaId : le.getLambdaIds())
                    {
                        context.lambdaTypes.remove(lambdaId);
                    }
                }

                // Resolve all the rest of the functions arguments that is left after we resolved the lambda expressions
                for (int i = 0; i < size; i++)
                {
                    if (arguments.get(i) == null)
                    {
                        IExpression arg = expression.getArguments()
                                .get(i);
                        arguments.set(i, arg.accept(this, context));
                    }
                }

                return new FunctionCallExpression(expression.getCatalogAlias(), expression.getFunctionInfo(), expression.getAggregateMode(), arguments);
            }

            return super.visit(expression, context);
        }

        @Override
        public IExpression visit(LambdaExpression expression, Ctx context)
        {
            throw new IllegalArgumentException("Lambda expressions should not be visited");
        }

        @Override
        public IExpression visit(AsteriskExpression expression, Ctx context)
        {
            String alias = expression.getQname()
                    .size() > 0 ? expression.getQname()
                            .getFirst()
                            : null;
            // Linked set because we need to add the table refs in the order they come
            // so expansion later on gets columns in correct order
            Set<TableSourceReference> tableSourceReferences = new LinkedHashSet<>();

            // Asterisks are resolved from either current schema or outer, not both
            ResolveSchema resolveSchema = context.schema;
            boolean outer = false;
            if (resolveSchema == null
                    || resolveSchema.isEmpty())
            {
                resolveSchema = context.outerSchema;
                outer = true;
            }

            if (resolveSchema != null)
            {
                // Resolve all aliases
                if (alias == null)
                {
                    int size = resolveSchema.getSize();
                    for (int i = 0; i < size; i++)
                    {
                        Column column = resolveSchema.getColumn(i)
                                .getRight();
                        boolean isInternal = SchemaUtils.isInternal(column);
                        // Internal column's table sources should not be included
                        if (isInternal)
                        {
                            continue;
                        }
                        TableSourceReference tableSource = SchemaUtils.getTableSource(column);
                        if (tableSource != null)
                        {
                            tableSourceReferences.add(tableSource);
                        }
                    }
                }
                else
                {
                    Pair<Integer, ResolveSchema> pair = resolveSchema.getResolveSchema(alias);
                    if (pair == null)
                    {
                        throw new ParseException("Alias " + alias + " could not be bound", expression.getLocation());
                    }

                    resolveSchema = pair.getValue();
                    if (outer)
                    {
                        Schema schema = resolveSchema.getSchema();
                        if (resolveSchema.isPopulated())
                        {
                            schema = resolveSchema.getColumn(0)
                                    .getValue()
                                    .getType()
                                    .getSchema();
                        }
                        context.outerReferences
                                .add(new CoreColumn(alias, ResolvedType.table(schema), "", false, SchemaUtils.getTableSource(schema), SchemaUtils.isAsterisk(schema) ? CoreColumn.Type.ASTERISK
                                        : CoreColumn.Type.REGULAR));
                    }
                    int size = resolveSchema.getSize();
                    for (int i = 0; i < size; i++)
                    {
                        Column column = resolveSchema.getColumn(i)
                                .getRight();
                        boolean isInternal = SchemaUtils.isInternal(column);
                        // Internal column's table sources should not be included
                        if (isInternal)
                        {
                            continue;
                        }
                        TableSourceReference tableSource = SchemaUtils.getTableSource(column);
                        if (tableSource != null)
                        {
                            tableSourceReferences.add(tableSource);
                        }
                    }
                }
            }

            return new AsteriskExpression(expression.getQname(), expression.getLocation(), tableSourceReferences);
        }

        @Override
        public IExpression visit(UnresolvedFunctionCallExpression expression, Ctx context)
        {
            throw new IllegalArgumentException("UnresolvedFunctionCallExpression should not be present at this stage");
        }

        @Override
        public IExpression visit(UnresolvedSubQueryExpression expression, ColumnResolver.Ctx context)
        {
            throw new IllegalArgumentException("UnresolvedSubQueryExpression should not be present at this stage");
        }

        /**
         * Resolve unbound column expression.
         * 
         * <pre>
         * Binds the column to input. Binding has this priority
         *  - Find a suitable schema to search in
         *    - Alias mapping for each table source
         *    - Outer schema if any (this also detects outer references)
         *    - If qualified name has no alias then we search in both current schema and outer schema
         * </pre>
         */
        @Override
        public IExpression visit(UnresolvedColumnExpression expression, ColumnResolver.Ctx context)
        {
            if (expression.getLambdaId() >= 0)
            {
                return resolveLambda(expression, context);
            }
            IExpression result = resolveColumn(expression, context);
            return result;
        }

        private IExpression resolveLambda(UnresolvedColumnExpression expression, ColumnResolver.Ctx context)
        {
            QualifiedName path = expression.getColumn();
            String alias = path.getFirst();
            path = path.extract(1);

            int lambdaId = expression.getLambdaId();

            ResolvedType lambdaType = context.lambdaTypes.get(lambdaId);

            Column.Type type = lambdaType.getType();

            // Applying lambda to a table yields an object with the same schema
            if (type == Column.Type.Table)
            {
                return DereferenceExpression.create(ColumnExpression.Builder.of(alias, ResolvedType.object(lambdaType.getSchema()))
                        .withLambdaId(lambdaId)
                        .build(), path, expression.getLocation());
            }
            // Applying a lambda to an array then we will get the sub type
            else if (type == Column.Type.Array)
            {
                return DereferenceExpression.create(ColumnExpression.Builder.of(alias, lambdaType.getSubType())
                        .withLambdaId(lambdaId)
                        .build(), path, expression.getLocation());
            }

            // All other types is resolved to that type
            return DereferenceExpression.create(ColumnExpression.Builder.of(alias, lambdaType)
                    .withLambdaId(lambdaId)
                    .build(), path, expression.getLocation());
        }

        private IExpression resolveColumn(UnresolvedColumnExpression expression, ColumnResolver.Ctx context)
        {
            // CSOFF
            ResolveSchema schema = context.schema;
            ResolveSchema outerSchema = context.outerSchema;
            // CSON

            QualifiedName path = expression.getColumn();
            String alias = Objects.toString(path.getAlias(), "")
                    .toLowerCase();
            path = "".equals(alias) ? path
                    : path.extract(1);

            String column = path.getFirst();
            path = path.extract(1);

            IExpression result = null;
            Pair<IExpression, Column> pair = null;
            if (schema != null)
            {
                pair = resolve(expression.getColumn(), schema, alias, column, null, expression.getLocation());
                result = pair != null ? pair.getKey()
                        : null;
            }

            boolean aliasMatch = !"".equals(alias)
                    && result != null;

            // Search in outer scope
            // NOTE! Schema is the same as the outer schema when resolving a projection with subexpressions
            if (outerSchema != null
                    && schema != outerSchema
                    && !aliasMatch)
            {
                IExpression maybeResult = result;
                boolean isAsterisk = pair != null
                        && SchemaUtils.isAsterisk(pair.getValue(), true);

                Set<Column> outerReferences = context.outerReferences;
                context.outerReferences = new HashSet<>();

                // Result is still null or
                // we found an asterisk match and we have an outer schema then we must search in outer too
                // since there could be a non asterisk match there which have higher precedence
                if (result == null
                        || isAsterisk)
                {
                    pair = resolve(expression.getColumn(), outerSchema, alias, column, context.outerReferences, expression.getLocation());
                    result = pair != null ? pair.getKey()
                            : null;
                }

                // Swap back to the first hit if outer did not yield a match
                // or that was also asterisk
                if (isAsterisk)
                {
                    result = getHighestPrecedence(maybeResult, pair);

                    // The result was the outer match, add the temp outer references to result
                    if (result != maybeResult
                            && outerReferences != null)
                    {
                        outerReferences.addAll(context.outerReferences);
                    }
                }
                else if (outerReferences != null)
                {
                    outerReferences.addAll(context.outerReferences);
                }
                // Restore the outer references
                context.outerReferences = outerReferences;
            }

            if (result == null)
            {
                // If we are on top we can bind non matching expressions to a single available schema
                if (!context.insideSubQuery)
                {
                    path = expression.getColumn();
                    column = path.getFirst();
                    path = path.extract(1);

                    // If we only have a single schema to choose from and no hit's
                    // bind the expression to that schema
                    if (schema != null
                            && (outerSchema == null
                                    || outerSchema.isEmpty())
                            && schema.schemas.size() == 1)
                    {
                        pair = resolve(expression.getColumn(), schema, schema.schemas.get(0).alias, column, null, expression.getLocation());
                        result = pair != null ? pair.getKey()
                                : null;
                    }
                    else if ((schema == null
                            || schema.isEmpty())
                            && outerSchema != null
                            && outerSchema.schemas.size() == 1)
                    {
                        pair = resolve(expression.getColumn(), outerSchema, outerSchema.schemas.get(0).alias, column, context.outerReferences, expression.getLocation());
                        result = pair != null ? pair.getKey()
                                : null;
                    }
                }

                if (result != null)
                {
                    return DereferenceExpression.create(result, path, expression.getLocation());
                }
                else
                {
                    // No expression => cannot be bound
                    throw new ParseException(expression.getColumn() + " cannot be bound", expression.getLocation());
                }
            }

            if (path.size() > 0)
            {
                return DereferenceExpression.create(result, path, expression.getLocation());
            }

            return result;
        }

        /** Returns the expression that has the highest precedence between a current match and outer schema match */
        private IExpression getHighestPrecedence(IExpression current, Pair<IExpression, Column> outer)
        {
            if (outer == null)
            {
                return current;
            }

            boolean isPopulated = SchemaUtils.isPopulated(outer.getValue());
            boolean isAsterisk = SchemaUtils.isAsterisk(outer.getValue(), true);

            // Current is asterisk here
            // Non asterisk or populated has higher precedence than asterisk
            if (!isAsterisk
                    || isPopulated)
            {
                return outer.getKey();
            }

            // Fallback to current if no distinct "winner" can be found
            return current;
        }

        private Pair<IExpression, Column> resolve(QualifiedName originalQname, ResolveSchema schema, String alias, String column, Set<Column> outerReferences, Location location)
        {
            int asteriskIndexMatch = -1;
            int nonAsteriskIndexMatch = -1;
            boolean multipleAsterisk = false;
            boolean canUseOrdinal = true;
            boolean aliasMatch = false;

            int size = schema.getSize();
            for (int i = 0; i < size; i++)
            {
                Pair<String, Column> p = schema.getColumn(i);

                Column schemaColumn = p.getValue();
                CoreColumn.Type columnType = SchemaUtils.getColumnType(schemaColumn);
                boolean isAsterisk = columnType == CoreColumn.Type.ASTERISK;
                String schemaAlias = p.getKey();
                TableSourceReference schemaTableRef = SchemaUtils.getTableSource(schemaColumn);
                String columnAlias = schemaTableRef != null ? schemaTableRef.getAlias()
                        : "";

                // Ordinals can be used if there are no asterisk in the schema
                // Named asterisks count as asterisk schema wise
                canUseOrdinal = canUseOrdinal
                        && !(columnType == CoreColumn.Type.ASTERISK
                                || columnType == CoreColumn.Type.NAMED_ASTERISK);

                // Match alias
                if (!"".equals(alias))
                {
                    // Non empty schema alias and the qualifier alias doesn't match => column not eligible
                    if (!"".equalsIgnoreCase(schemaAlias)
                            && !alias.equalsIgnoreCase(schemaAlias))
                    {
                        continue;
                    }
                    // Empty schema alias and the qualifier alias doesn't match column alias => column not eligible
                    else if ("".equalsIgnoreCase(schemaAlias)
                            && !alias.equalsIgnoreCase(columnAlias))
                    {
                        continue;
                    }
                }

                if (isAsterisk)
                {
                    // We cannot throw until after loop since a named column has higher prio
                    // and then it's ok to have multiple asterisk matches
                    multipleAsterisk = asteriskIndexMatch >= 0;
                    asteriskIndexMatch = i;
                }
                else if (column.equalsIgnoreCase(schemaColumn.getName())
                        || (SchemaUtils.isPopulated(schemaColumn)
                                && schemaColumn.getName()
                                        .equalsIgnoreCase(alias)))
                {

                    // Multiple matches of named columns is not allowed
                    if (nonAsteriskIndexMatch >= 0)
                    {
                        throw new ParseException("Ambiguous column: " + originalQname, location);
                    }
                    nonAsteriskIndexMatch = i;
                    aliasMatch = schemaColumn.getName()
                            .equalsIgnoreCase(alias);

                    // If we have a match with an alias we don't need to search any more
                    if (alias != null
                            && StringUtils.equalsIgnoreCase(alias, columnAlias))
                    {
                        break;
                    }

                }
            }

            if (nonAsteriskIndexMatch == -1
                    && multipleAsterisk)
            {
                throw new ParseException("Ambiguous column: " + originalQname, location);
            }
            else if (nonAsteriskIndexMatch == -1
                    && asteriskIndexMatch == -1)
            {
                return null;
            }

            int index = nonAsteriskIndexMatch >= 0 ? nonAsteriskIndexMatch
                    : asteriskIndexMatch;

            TableSourceReference schemaTableSource = schema.getTableSource(index);

            Column match = schema.getColumn(index)
                    .getRight();

            int ordinal = canUseOrdinal ? index
                    : -1;

            ColumnExpression.Builder builder = ColumnExpression.Builder.of(aliasMatch ? alias
                    : column, match.getType())
                    .withOrdinal(ordinal);

            if (ordinal < 0)
            {
                builder.withColumn(aliasMatch ? alias
                        : column);
            }

            TableSourceReference tableRef = SchemaUtils.getTableSource(match);

            // If column did not have a table source use the one from schema (sub query)
            if (tableRef == null)
            {
                tableRef = schemaTableSource;
            }

            if (tableRef != null)
            {
                CoreColumn.Type columnType = SchemaUtils.getColumnType(match);

                // Switch to a named asterisk upon match since we cannot have asterisk columns
                // further down
                if (columnType == CoreColumn.Type.ASTERISK)
                {
                    columnType = CoreColumn.Type.NAMED_ASTERISK;
                }

                match = new CoreColumn(column, match.getType(), "", false, tableRef, columnType);
                builder.withColumnReference(new ColumnReference(tableRef, columnType));
            }

            // Add column to outer references and set outer reference to column builder
            if (outerReferences != null)
            {
                builder.withOuterReference(true);
                outerReferences.add(match);
            }

            // If we had a match on the alias we need to wrap the column expression with a dereference of the column
            if (aliasMatch)
            {
                return Pair.of(DereferenceExpression.create(builder.build(), QualifiedName.of(column), location), match);
            }

            return Pair.of(builder.build(), match);
        }
    }

    /** Wrapper around a list of schemas used during resolving */
    static class ResolveSchema
    {
        final List<AliasSchema> schemas = new ArrayList<>();

        ResolveSchema()
        {
        }

        /** Construct a resolve schema from a single regular schema */
        ResolveSchema(Schema schema)
        {
            this(schema, "");
        }

        /** Construct a resolve schema from a single regular schema */
        ResolveSchema(Schema schema, String alias)
        {
            schemas.add(new AliasSchema(schema, alias));
        }

        /** Copy a resolve schema but change it's alias */
        ResolveSchema(ResolveSchema schema, TableSourceReference tableSource)
        {
            // Concat all the actual schemas into one alias schema
            Schema result = null;

            for (AliasSchema s : schema.schemas)
            {
                if (result == null)
                {
                    result = s.schema;
                }
                else
                {
                    result = SchemaUtils.joinSchema(result, s.schema);
                }
            }

            schemas.add(new AliasSchema(result, tableSource));
        }

        boolean isEmpty()
        {
            return schemas.isEmpty()
                    || (schemas.size() == 1
                            && schemas.get(0).schema.getSize() == 0);
        }

        /** Create a regular schema from this instance */
        Schema getSchema()
        {
            List<Column> columns = new ArrayList<>(getSize());

            for (AliasSchema s : schemas)
            {
                for (Column column : s.schema.getColumns())
                {
                    columns.add(column);
                }
            }

            return new Schema(columns);
        }

        /** Get sub schema with provided alias */
        Pair<Integer, ResolveSchema> getResolveSchema(String alias)
        {
            int index = 0;
            for (AliasSchema s : schemas)
            {
                if (alias.equalsIgnoreCase(s.alias))
                {
                    return Pair.of(index, new ResolveSchema(s.schema));
                }
                index += s.schema.getSize();
            }
            return null;
        }

        int getSize()
        {
            int size = 0;
            for (AliasSchema s : schemas)
            {
                size += s.schema.getSize();
            }
            return size;
        }

        Pair<String, Column> getColumn(int index)
        {
            int count = 0;
            for (AliasSchema s : schemas)
            {
                int size = s.schema.getSize();
                for (int i = 0; i < size; i++)
                {
                    Column column = s.schema.getColumns()
                            .get(i);
                    if (count == index)
                    {
                        return Pair.of(s.alias, column);
                    }
                    count++;
                }
            }
            return null;
        }

        TableSourceReference getTableSource(int index)
        {
            int count = 0;
            for (AliasSchema s : schemas)
            {
                int size = s.schema.getSize();
                for (int i = 0; i < size; i++)
                {
                    if (count == index)
                    {
                        return s.tableSource;
                    }
                    count++;
                }
            }
            return null;
        }

        boolean isPopulated()
        {
            return getSize() == 1
                    && SchemaUtils.isPopulated(getColumn(0).getValue());
        }

        boolean hasAsteriskColumns()
        {
            for (AliasSchema s : schemas)
            {
                if (SchemaUtils.isAsterisk(s.schema))
                {
                    return true;
                }
            }
            return false;
        }

        /** Append alias schema */
        void add(AliasSchema aliasSchema, Location location)
        {
            if (!"".equalsIgnoreCase(aliasSchema.alias)
                    && schemas.stream()
                            .anyMatch(s -> equalsAnyIgnoreCase(s.alias, aliasSchema.alias)))
            {
                throw new ParseException("Alias '" + aliasSchema.alias + "' is specified multiple times.", location);
            }
            schemas.add(aliasSchema);
        }

        void addAll(List<AliasSchema> schemas, Location location)
        {
            for (AliasSchema schema : schemas)
            {
                add(schema, location);
            }
        }

        static ResolveSchema concat(ResolveSchema schema1, ResolveSchema schema2)
        {
            if (schema1 == null)
            {
                return schema2;
            }

            ResolveSchema result = new ResolveSchema();
            result.schemas.addAll(schema1.schemas);
            result.schemas.addAll(schema2.schemas);
            return result;
        }
    }

    /** Wrapper around a schema with an attached alias to property resolve qualifiers */
    static class AliasSchema
    {
        final Schema schema;
        final String alias;
        TableSourceReference tableSource;

        AliasSchema(Schema schema, TableSourceReference tableSource)
        {
            this(schema, tableSource.getAlias());
            this.tableSource = tableSource;
        }

        AliasSchema(Schema schema, String alias)
        {
            this.schema = schema;
            this.alias = alias;
        }
    }
}
