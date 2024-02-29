package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.equalsAnyIgnoreCase;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
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
import se.kuseman.payloadbuilder.api.expression.IDereferenceExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.AExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.expression.LogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedFunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedSubQueryExpression;
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
        Set<ColumnReference> aggregateColumnReferences;

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

        ResolveSchema schema = context.getSchema(input);

        // Concat the schemas here, if we visit sub query expression further down
        // this will be the actual outer schema for that sub plan
        // This is a preparation for detecting a correlated sub query expression that will
        // be optimized later on in SubQueryExpressionPushDown rule
        // CSOFF
        ResolveSchema prevOuterSchema = context.outerSchema;
        // CSON
        MutableBoolean hasSubQueries = new MutableBoolean(false);
        plan.getExpressions()
                .forEach(e -> e.accept(SubQueryExpressionDetector.INSTANCE, hasSubQueries));
        if (hasSubQueries.booleanValue())
        {
            context.outerSchema = ResolveSchema.concat(context.outerSchema, schema);
        }
        // Rewrite expressions according to input plan schema
        context.schema = schema;
        List<IExpression> expressions = ColumnResolverVisitor.rewrite(context, plan.getExpressions());

        // Expand all asterisks, they should not be left for execution
        expressions = expandAsterisks(expressions, context.outerReferences, context.outerSchema, schema);

        if (input instanceof Projection)
        {
            expressions = ProjectionMerger.replace(expressions, (Projection) input);
            input = ((Projection) input).getInput();
        }

        context.outerSchema = prevOuterSchema;

        // Might create the resulting schema from the expressions here and not do that on demand
        ILogicalPlan result = new Projection(input, expressions, plan.isAppendInputColumns());
        context.planSchema.push(new ResolveSchema(result.getSchema()));
        return result;
    }

    @Override
    public ILogicalPlan visit(Filter plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);
        ResolveSchema schema = context.getSchema(input);

        // Rewrite predicate according to input plan schema
        context.schema = schema;
        IExpression predicate = ColumnResolverVisitor.rewrite(context, plan.getPredicate());

        // Nested filters => AND the predicates and return plans input
        if (input instanceof Filter)
        {
            IExpression inputPredicate = ((Filter) input).getPredicate();
            predicate = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, inputPredicate, predicate);
            input = ((Filter) input).getInput();
        }

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

        // Outer reference are only supported in outer/cross apply's
        if (plan.getCondition() == null
                && plan.getType() != Join.Type.CROSS)
        {
            context.outerSchema = ResolveSchema.concat(context.outerSchema, leftSchema);
            context.outerReferences = new HashSet<>();
        }
        else
        {
            context.outerSchema = null;
            context.outerReferences = null;
        }

        ILogicalPlan right = plan.getInner()
                .accept(this, context);

        ResolveSchema rightSchema = context.getSchema(right);

        // Construct a resulting join schema
        ResolveSchema resultSchema = new ResolveSchema();
        resultSchema.schemas.addAll(leftSchema.schemas);
        if (plan.getPopulateAlias() != null)
        {
            Schema populateSchema = rightSchema.getSchema();
            ColumnReference colRef = SchemaUtils.getColumnReference(populateSchema.getColumns()
                    .get(0));
            if (colRef != null)
            {
                colRef = colRef.rename(plan.getPopulateAlias());
            }

            resultSchema.add(new AliasSchema(Schema.of(CoreColumn.of(plan.getPopulateAlias(), ResolvedType.table(populateSchema), colRef)), plan.getPopulateAlias()), null);
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
        Join result = new Join(left, right, plan.getType(), plan.getPopulateAlias(), condition, context.outerReferences, plan.isSwitchedInputs());
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
        context.aggregateColumnReferences = new HashSet<>();
        for (IExpression e : aggregateExpressions)
        {
            if (e instanceof HasColumnReference)
            {
                ColumnReference colRef = ((HasColumnReference) e).getColumnReference();
                if (colRef != null)
                {
                    context.aggregateColumnReferences.add(colRef);
                }
            }
        }

        List<IAggregateExpression> projectionExpressions = new ArrayList<>();
        int size = plan.getProjectionExpressions()
                .size();
        for (int i = 0; i < size; i++)
        {
            IExpression exp = ColumnResolverVisitor.rewrite(context, plan.getProjectionExpressions()
                    .get(i));

            if (exp instanceof AggregateWrapperExpression
                    && ((AggregateWrapperExpression) exp).getExpression() instanceof AsteriskExpression)
            {
                AggregateWrapperExpression awe = (AggregateWrapperExpression) exp;

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
            boolean isAsterisk = SchemaUtils.isAsterisk(column);
            if ("".equals(column.getName())
                    && !isAsterisk)
            {
                throw new ParseException("Missing column name for ordinal " + i + " of " + plan.getAlias(), plan.getLocation());
            }
        }
        context.planSchema.push(new ResolveSchema(schema, plan.getAlias()));
        context.insideSubQuery = prevInsideSubQuery;
        return result;
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
                .resolveTableFunctionInfo(plan.getCatalogAlias(), plan.getTableSource()
                        .getName()
                        .getFirst())
                .getValue();

        // Resolve the table function schema from it's resolved arguments
        Schema schema = Schema.EMPTY;
        try
        {
            schema = function.getSchema(expressions);
        }
        catch (SchemaResolveException e)
        {
            throw new ParseException(e.getMessage(), plan.getLocation());
        }
        if (schema.getColumns()
                .isEmpty())
        {
            ColumnReference ast = new ColumnReference(plan.getTableSource(), plan.getAlias(), ColumnReference.Type.ASTERISK);
            schema = Schema.of(CoreColumn.of(ast, ResolvedType.of(Type.Any)));
        }
        else
        {
            // .. force all columns to have a proper column reference
            schema = SchemaResolver.recreate(plan.getTableSource(), schema);
        }

        context.planSchema.push(new ResolveSchema(schema, plan.getTableSource()
                .getAlias()));

        List<Option> options = plan.getOptions()
                .stream()
                .map(o -> new Option(o.getOption(), ColumnResolverVisitor.rewrite(context, o.getValueExpression())))
                .toList();

        return new TableFunctionScan(plan.getTableSource(), schema, expressions, options, plan.getLocation());
    }

    @Override
    public ILogicalPlan visit(TableScan plan, Ctx context)
    {
        List<Option> options = plan.getOptions()
                .stream()
                .map(o -> new Option(o.getOption(), ColumnResolverVisitor.rewrite(context, o.getValueExpression())))
                .toList();

        context.planSchema.push(new ResolveSchema(plan.getSchema(), plan.getTableSource()
                .getAlias()));

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

        Schema schema = type.getSchema();
        if (schema == null)
        {
            schema = Schema.EMPTY;
        }

        if (SchemaUtils.isAsterisk(schema, true))
        {
            // Set an asterisk schema with expression scans table source
            ColumnReference ast = new ColumnReference(plan.getTableSource(), plan.getAlias(), ColumnReference.Type.ASTERISK);
            schema = Schema.of(CoreColumn.of(ast, ResolvedType.of(Type.Any)));
        }
        else
        {
            // .. else force all columns to have a proper column reference
            schema = SchemaResolver.recreate(plan.getTableSource(), schema);
        }

        context.schema = prevSchema;

        context.planSchema.push(new ResolveSchema(schema, plan.getTableSource()
                .getAlias()));

        return new ExpressionScan(plan.getTableSource(), schema, expression, plan.getLocation());
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

        // Create a temporary column name, this will be corrected in SubQueryExpressionPushDown later on
        Schema schema = Schema.of(Column.of("output", pair.getValue()
                .getType(input.getSchema())));
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
                ColumnReference populatedColRef = null;
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
                        if (pair == null)
                        {
                            throw new ParseException("Alias " + qname + " could not be bound", ae.getLocation());
                        }

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

                int size = schema.getSize();
                // Populated, dig into populate schema
                if (schema.isPopulated())
                {
                    if (outerReference)
                    {
                        outerReferences.add(schema.getColumn(0)
                                .getValue());
                    }
                    populateIndex = startIndex;
                    populateAlias = qname.getFirst()
                            .toLowerCase();
                    schema = new ResolveSchema(schema.getColumn(0)
                            .getValue()
                            .getType()
                            .getSchema());

                    // The column of a populated join uses the first columns column reference of the populated schema
                    // and we need to set that column reference on all expanded column expressions
                    // to properly make ColumnExpression#eval work
                    populatedColRef = SchemaUtils.getColumnReference(schema.getColumn(0)
                            .getValue());
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

                    // We use the first columns colref if it's a populated schema
                    ColumnReference colRef = populatedColRef != null ? populatedColRef
                            : SchemaUtils.getColumnReference(column);

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

                    // A asterisk column (a.*) then we don't have a path but only a column reference
                    // this will be expanded further runtime by the projection
                    if (colRef != null
                            && colRef.isAsterisk())
                    {
                        // Cannot use ordinals when having asterisks
                        index = -1;
                        path = QualifiedName.of();
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
                            .withColumnReference(colRef)
                            .withOuterReference(outerReference)
                            .withOrdinal(index);

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

    private static class SubQueryExpressionDetector extends AExpressionVisitor<Void, MutableBoolean>
    {
        private static final SubQueryExpressionDetector INSTANCE = new SubQueryExpressionDetector();

        @Override
        public Void visit(UnresolvedSubQueryExpression expression, MutableBoolean context)
        {
            context.setTrue();
            // No need to visit the subquery nestedly since we found one already
            return null;
        }
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
            /*
             * If this is a wrapper around an asterisk expression then simply return the input. This will be expanded into column expressions later on and the singleValue property on the wrapper
             * should be not be altered with because it's used when creating the expanded expressions
             */
            if (expression.getExpression() instanceof AsteriskExpression)
            {
                return expression;
            }

            IExpression result = expression.getExpression()
                    .accept(this, context);

            Set<ColumnReference> columnReferences = ColumnResolver.collectColumns(singletonList(result));

            boolean singleValue = columnReferences.isEmpty()
                    || CollectionUtils.containsAll(context.aggregateColumnReferences, columnReferences);

            return new AggregateWrapperExpression(result, singleValue, expression.isInternal());
        }

        @Override
        public IExpression visit(IDereferenceExpression expression, Ctx context)
        {
            return DereferenceExpression.create(expression.getExpression()
                    .accept(this, context), QualifiedName.of(expression.getRight()), null);
        };

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
        public IExpression visit(UnresolvedFunctionCallExpression expression, Ctx context)
        {
            // Throw here catch errors early on if something was missed in schema resolver
            throw new IllegalArgumentException("An unresolved function call should not exist at this stage. Verify SchemaResolver");
        }

        /**
         * Dig down into sub query and resolve Since this will be a nested loop further down we collect outer references
         */
        @Override
        public IExpression visit(UnresolvedSubQueryExpression expression, ColumnResolver.Ctx context)
        {
            // Reset the outer reference list before visiting the sub query expression plan
            // to distinctly gather outer references for this sub query
            // CSOFF
            Set<Column> prevOuterReferences = context.outerReferences;
            // CSON
            context.outerReferences = new HashSet<>();

            // Resolve the sub query plan
            ILogicalPlan input = expression.getInput()
                    .accept(context.columnResvoler, context);

            // CSOFF
            IExpression result = new UnresolvedSubQueryExpression(input, context.outerReferences, expression.getLocation());
            // CSON

            // Pop the top schema after we visit the logical plan to keep a clean state
            context.planSchema.pop();

            // Restore context values
            // Add all new outer references to previous list to cascade the list upwards
            if (prevOuterReferences != null)
            {
                prevOuterReferences.addAll(context.outerReferences);
            }
            context.outerReferences = prevOuterReferences;

            return result;
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
            IExpression result = visitInternal(expression, context);
            return result;
        }

        private IExpression visitInternal(UnresolvedColumnExpression expression, ColumnResolver.Ctx context)
        {
            // CSOFF
            ResolveSchema schema = context.schema;
            ResolveSchema outerSchema = context.outerSchema;
            // CSON

            QualifiedName path = expression.getColumn();
            String alias;

            int lambdaId = expression.getLambdaId();
            if (lambdaId >= 0)
            {
                alias = path.getFirst();
                path = path.extract(1);

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

            alias = Objects.toString(path.getAlias(), "")
                    .toLowerCase();
            path = "".equals(alias) ? path
                    : path.extract(1);

            String column = path.getFirst();
            path = path.extract(1);

            IExpression result = null;
            if (schema != null)
            {
                result = resolve(expression.getColumn(), schema, alias, column, null, lambdaId, expression.getLocation());
            }

            // Search in outer scope
            // NOTE! Schema is the same as the outer schema when resolving a projection with subexpressions
            if (outerSchema != null
                    && schema != outerSchema)
            {
                IExpression maybeResult = result;
                ColumnReference colRef = null;
                if (maybeResult instanceof HasColumnReference)
                {
                    colRef = ((HasColumnReference) maybeResult).getColumnReference();
                }

                boolean asteriskMatch = result != null
                        && colRef != null
                        && colRef.getType() == ColumnReference.Type.NAMED_ASTERISK;

                Set<Column> outerReferences = context.outerReferences;
                context.outerReferences = new HashSet<>();

                // Result is still null or
                // we found an asterisk match and we have an outer schema then we must search in outer too
                // since there could be a non asterisk match there which have higher precedence
                if (result == null
                        || asteriskMatch)
                {
                    result = resolve(expression.getColumn(), outerSchema, alias, column, context.outerReferences, -1, expression.getLocation());
                }

                // Swap back to the first hit if outer did not yield a match
                // or that was also asterisk
                if (asteriskMatch)
                {
                    result = getHighestPrecedence(maybeResult, result);

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
                        result = resolve(expression.getColumn(), schema, schema.schemas.get(0).alias, column, null, -1, expression.getLocation());
                    }
                    else if ((schema == null
                            || schema.isEmpty())
                            && outerSchema != null
                            && outerSchema.schemas.size() == 1)
                    {
                        result = resolve(expression.getColumn(), outerSchema, outerSchema.schemas.get(0).alias, column, context.outerReferences, -1, expression.getLocation());
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
        private IExpression getHighestPrecedence(IExpression current, IExpression outer)
        {
            if (outer == null)
            {
                return current;
            }

            ColumnReference colRef = null;
            if (outer instanceof HasColumnReference)
            {
                colRef = ((HasColumnReference) outer).getColumnReference();
            }

            boolean outerAsteriskMatch = outer != null
                    && colRef != null
                    && colRef.getType() == ColumnReference.Type.NAMED_ASTERISK;

            // Current is asterisk here
            // Non asterisk outer has higher precedence than asterisk
            if (!outerAsteriskMatch)
            {
                return outer;
            }

            // Check if the outer is a match on outer table alias from a populated column
            if (outer instanceof ColumnExpression)
            {
                ColumnExpression ce = (ColumnExpression) outer;
                colRef = ce.getColumnReference();
                if (colRef != null)
                {
                    TableSourceReference tableSource = colRef.getTableSource();
                    String alias = tableSource.getAlias();
                    if (ce.getColumn()
                            .equalsIgnoreCase(alias))
                    {
                        return outer;
                    }
                }
            }

            // Fallback to current if no distinct "winner" can be found
            return current;
        }

        private IExpression resolve(QualifiedName originalQname, ResolveSchema schema, String alias, String column, Set<Column> outerReferences, int lambdaId, Location location)
        {
            int asteriskIndexMatch = -1;
            int nonAsteriskIndexMatch = -1;
            boolean multipleAsterisk = false;
            boolean populatedColumnMatch = false;
            boolean canUseOrdinal = true;

            int size = schema.getSize();
            for (int i = 0; i < size; i++)
            {
                Pair<String, Column> p = schema.getColumn(i);

                Column schemaColumn = p.getValue();
                String schemaAlias = p.getKey();
                ColumnReference schemaColRef = SchemaUtils.getColumnReference(schemaColumn);

                String columnAlias = schemaColRef != null ? schemaColRef.getTableSource()
                        .getAlias()
                        : "";
                boolean isAsterisk = SchemaUtils.isAsterisk(schemaColumn);

                // Ordinals can be used if there are no asterisk in the schema
                canUseOrdinal = canUseOrdinal
                        && (schemaColRef == null
                                || schemaColRef.getType() == ColumnReference.Type.REGULAR);

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
                        || (schemaColumn.getType()
                                .getType() == Column.Type.Table
                                && schemaColumn.getName()
                                        .equalsIgnoreCase(alias)))
                {
                    // Multiple matches of named columns is not allowed
                    if (nonAsteriskIndexMatch >= 0)
                    {
                        throw new ParseException("Ambiguous column: " + originalQname, location);
                    }
                    nonAsteriskIndexMatch = i;

                    populatedColumnMatch = schemaColumn.getName()
                            .equalsIgnoreCase(alias);
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

            Column match = schema.getColumn(index)
                    .getRight();

            int ordinal = canUseOrdinal ? index
                    : -1;

            ColumnExpression.Builder builder = ColumnExpression.Builder.of(populatedColumnMatch ? alias
                    : column, match.getType())
                    .withOrdinal(ordinal)
                    .withLambdaId(lambdaId);

            if (ordinal < 0
                    && !"".equals(column))
            {
                builder.withColumn(populatedColumnMatch ? alias
                        : column);
            }

            ColumnReference colRef = SchemaUtils.getColumnReference(match);

            if (colRef != null)
            {
                // Name the asterisk column
                // or change a populated fictive column name to the actual column name
                if (!"".equals(column)
                        && (colRef.isAsterisk()
                                || populatedColumnMatch))
                {
                    colRef = colRef.rename(column);
                }
                match = CoreColumn.of(colRef.getName(), match.getType(), colRef);
                builder.withColumnReference(colRef);
            }

            // Add column to outer references and set outer reference to column builder
            if (outerReferences != null)
            {
                builder.withOuterReference(true);
                outerReferences.add(match);
            }

            // If we had a match on the alias we need to wrap the column expression with a dereference of the column
            if (populatedColumnMatch)
            {
                return DereferenceExpression.create(builder.build(), QualifiedName.of(column), location);
            }

            return builder.build();
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
        ResolveSchema(ResolveSchema schema, String alias)
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
                    result = SchemaUtils.concat(result, s.schema);
                }
            }

            schemas.add(new AliasSchema(result, alias));
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

        boolean isPopulated()
        {
            return getSize() == 1
                    && getColumn(0).getValue()
                            .getType()
                            .getType() == Type.Table;
        }

        boolean hasAsteriskColumns()
        {
            for (AliasSchema s : schemas)
            {
                for (Column column : s.schema.getColumns())
                {
                    ColumnReference colRef = SchemaUtils.getColumnReference(column);
                    if (colRef != null
                            && colRef.isAsterisk())
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        /** Append alias schema */
        void add(AliasSchema aliasSchema, Location location)
        {
            if (schemas.stream()
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

        AliasSchema(Schema schema, String alias)
        {
            this.schema = schema;
            this.alias = alias;
        }
    }
}
