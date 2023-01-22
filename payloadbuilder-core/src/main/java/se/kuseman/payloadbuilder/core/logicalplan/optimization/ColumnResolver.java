package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.defaultString;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.v4.runtime.Token;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IDereferenceExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.common.Option;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.AExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.expression.LogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.expression.SubQueryExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedFunctionCallExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.OverScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.SubQuery;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.parser.ParseException;

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
        Schema schema;

        /** Concated outer schema used by joins/sub query expressions to detect outer column references in {@link ColumnResolverVisitor} */
        Schema outerSchema;

        /** Outer references found during traversal. Used to detect correlated sub queries. Added in {@link ColumnResolverVisitor} */
        Set<Column> outerReferences;

        /** Schemas produced */
        Deque<Schema> schemas = new ArrayDeque<>();

        /**
         * Set with column references used when resolving aggregate projection to detect if an expression should be single value or grouped
         */
        Set<ColumnReference> aggregateColumnReferences;

        /**
         * Map of the schema for each alias for a single query level. Used when exploding asterisk expressions and column resolving etc.
         */
        Deque<Map<String, SchemaMapping>> schemaByAlias = new ArrayDeque<>();

        Ctx(IExecutionContext context, ColumnResolver columnResolver)
        {
            super(context);
            this.schemaByAlias.push(new HashMap<>());
            this.columnResvoler = columnResolver;
        }

        /** Pushes an alias mapping to the top of the alias mappings stack */
        void pushAliasSchema(String alias, Schema schema, boolean populated, Token token)
        {
            if (schemaByAlias.peek()
                    .put(alias.toLowerCase(), new SchemaMapping(schema, populated)) != null)
            {
                throw new ParseException("Alias '" + alias + "' is specified multiple times.", token);
            }
        }

        /** Pushes an alias mapping to the top of the alias mappings stack */
        void pushLambdaType(String alias, ResolvedType type, Token token)
        {
            SchemaMapping mapping = new SchemaMapping(Schema.EMPTY, false);
            mapping.lambdaType = type;
            if (schemaByAlias.peek()
                    .put(alias.toLowerCase(), mapping) != null)
            {
                throw new ParseException("Alias '" + alias + "' is specified multiple times.", token);
            }
        }
    }

    static class SchemaMapping
    {
        final Schema schema;
        final boolean populated;
        /** Type used for lambda aliases */
        ResolvedType lambdaType;

        SchemaMapping(Schema schema, boolean populated)
        {
            this.schema = schema;
            this.populated = populated;
        }

        @Override
        public String toString()
        {
            return schema.toString() + (populated ? " populated"
                    : "");
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

        Schema schema = input.getSchema();

        // Concat the schemas here, if we visit sub query expression further down
        // this will be the actual outer schema for that sub plan
        // This is a preparation for detecting a correlated sub query expression that will
        // be optimized later on in SubQueryExpressionPushDown rule
        // CSOFF
        Schema prevOuterSchema = context.outerSchema;
        // CSON
        MutableBoolean hasSubQueries = new MutableBoolean(false);
        plan.getExpressions()
                .forEach(e -> e.accept(SubQueryExpressionDetector.INSTANCE, hasSubQueries));
        if (hasSubQueries.booleanValue())
        {
            context.outerSchema = Schema.concat(context.outerSchema, schema);
        }
        // Rewrite expressions according to input plan schema
        context.schema = schema;
        List<IExpression> expressions = ColumnResolverVisitor.rewrite(context, plan.getExpressions());

        // Expand all asterisks, they should not be left for execution
        expressions = expandAsterisks(expressions, context.outerReferences, context.outerSchema, schema, context.schemaByAlias.peek());

        if (input instanceof Projection)
        {
            expressions = ProjectionMerger.replace(expressions, (Projection) input);
            input = ((Projection) input).getInput();
        }

        context.outerSchema = prevOuterSchema;

        // Might create the resulting schema from the expressions here and not do that on demand
        return new Projection(input, expressions, plan.isAppendInputColumns());
    }

    @Override
    public ILogicalPlan visit(Filter plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);
        Schema schema = input.getSchema();

        // Rewrite predicate according to input plan schema
        context.schema = schema;
        IExpression predicate = ColumnResolverVisitor.rewrite(context, plan.getPredicate());

        // Nested filters => AND predicates and return plans input
        if (input instanceof Filter)
        {
            IExpression inputPredicate = ((Filter) input).getPredicate();
            predicate = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, inputPredicate, predicate);
            input = ((Filter) input).getInput();
        }

        return new Filter(input, plan.getTableSource(), predicate);
    }

    @Override
    public ILogicalPlan visit(Sort plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);
        Schema schema = input.getSchema();

        if (input instanceof Sort)
        {
            // TODO: in populated join then this is valid
            throw new ParseException("Sort is invalid in sub queries", null);
        }

        // Rewrite sort items expressions
        context.schema = schema;
        List<SortItem> sortItems = plan.getSortItems()
                .stream()
                .map(si -> new SortItem(ColumnResolverVisitor.rewrite(context, si.getExpression()), si.getOrder(), si.getNullOrder(), si.getToken()))
                .collect(toList());

        return new Sort(input, sortItems);
    }

    @Override
    public ILogicalPlan visit(Join plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan left = plan.getOuter()
                .accept(this, context);

        // CSOFF
        // Concat outer and left schema to detect outer references
        Schema prevOuterSchema = context.outerSchema;
        // Reset the outer reference list before visiting left to distinctly
        // know what references this join has
        Set<Column> prevOuterReferences = context.outerReferences;
        // CSON

        Schema leftSchema = left.getSchema();

        // Outer reference are only supported in outer/cross apply's/cross joins (ie. there is no condition)
        if (plan.getCondition() == null)
        {
            context.outerSchema = Schema.concat(context.outerSchema, leftSchema);
            context.outerReferences = new HashSet<>();
        }
        else
        {
            context.outerSchema = null;
            context.outerReferences = null;
        }

        ILogicalPlan right = plan.getInner()
                .accept(this, context);

        IExpression condition = plan.getCondition();

        // Rewrite condition if present. Cross/outer joins doesn't have conditions
        if (condition != null)
        {
            context.schema = Schema.concat(leftSchema, right.getSchema());
            condition = ColumnResolverVisitor.rewrite(context, condition);
        }

        // Correct the inner schema mappings if we have a populate join
        // Note! The join condition is performed against a non populated schema, it's not until
        // after the join we have a populated schema, that is why we adjust after we resolved the condition columns
        String populateAlias = plan.getPopulateAlias();
        if (populateAlias != null)
        {
            SchemaMapping populateSchema = context.schemaByAlias.peek()
                    .remove(populateAlias.toLowerCase());

            // Create a column reference (named from the populate alias) from the inner table source, if any exists
            ColumnReference colRef = populateSchema.schema.getColumns()
                    .get(0)
                    .getColumnReference();
            if (colRef != null)
            {
                colRef = colRef.rename(populateAlias);
            }

            context.pushAliasSchema(populateAlias, Schema.of(new Column(populateAlias, ResolvedType.tupleVector(populateSchema.schema), colRef)), true, null);
        }

        // CSOFF
        Join result = new Join(left, right, plan.getType(), plan.getPopulateAlias(), condition, context.outerReferences, plan.isSwitchedInputs());
        // CSON

        // Restore context values
        context.outerSchema = prevOuterSchema;
        // Add all new outer references to previous list to cascade the list upwards
        if (prevOuterReferences != null)
        {
            prevOuterReferences.addAll(context.outerReferences);
        }
        context.outerReferences = prevOuterReferences;

        return result;
    }

    @Override
    public ILogicalPlan visit(Aggregate plan, Ctx context)
    {
        // Re-write input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        Schema schema = input.getSchema();
        // Resolve expressions against input schema
        context.schema = schema;

        List<IExpression> aggregateExpressions = ColumnResolverVisitor.rewrite(context, plan.getAggregateExpressions());
        // Expand asterisks, this only happens when aggregate is used for DISTINCT since GROUP BY * is invalid
        aggregateExpressions = expandAsterisks(aggregateExpressions, context.outerReferences, context.outerSchema, schema, context.schemaByAlias.peek());

        context.aggregateColumnReferences = new HashSet<>();
        for (IExpression e : aggregateExpressions)
        {
            if (e instanceof ColumnExpression)
            {
                ColumnExpression ce = (ColumnExpression) e;
                if (ce.getColumnReference() != null)
                {
                    context.aggregateColumnReferences.add(ce.getColumnReference());
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
                List<IExpression> expandAsterisks = expandAsterisks(singletonList(((AggregateWrapperExpression) exp).getExpression()), context.outerReferences, context.outerSchema, schema,
                        context.schemaByAlias.peek());
                for (IExpression ea : expandAsterisks)
                {
                    projectionExpressions.add(new AggregateWrapperExpression(ea, true, false));
                }
            }
            else
            {
                projectionExpressions.add((IAggregateExpression) exp);
            }
        }

        return new Aggregate(input, aggregateExpressions, projectionExpressions);
    }

    @Override
    public ILogicalPlan visit(SubQuery plan, Ctx context)
    {
        // Prepare to store all child schemas
        context.schemaByAlias.push(new HashMap<>());

        // Eliminate sub query, no sub queries should be left
        ILogicalPlan result = plan.getInput()
                .accept(this, context);

        Schema schema = result.getSchema();

        // Validate sub query schema, all columns must have a specified alias
        int size = schema.getSize();
        for (int i = 0; i < size; i++)
        {
            Column column = schema.getColumns()
                    .get(i);

            if ("".equals(column.getName())
                    && (column.getColumnReference() == null
                            || !column.getColumnReference()
                                    .isAsterisk()))
            {
                throw new ParseException("Missing column name for ordinal " + i + " of " + plan.getAlias(), plan.getToken());
            }
        }

        // Pop the map we put all child schemas in since those are not needed anymore when we leave the subquery
        context.schemaByAlias.pop();
        context.pushAliasSchema(plan.getAlias(), schema, false, plan.getToken());
        return result;
    }

    @Override
    public ILogicalPlan visit(TableFunctionScan plan, Ctx context)
    {
        // CSOFF
        Schema prevSchema = context.schema;
        // Table function only have outer schema to search values from
        Deque<Map<String, SchemaMapping>> prevSchemaByAlias = context.schemaByAlias;
        context.schema = null;
        context.schemaByAlias = new ArrayDeque<>();
        List<IExpression> expressions = ColumnResolverVisitor.rewrite(context, plan.getArguments());
        context.schemaByAlias = prevSchemaByAlias;
        // CSON

        context.pushAliasSchema(plan.getAlias(), plan.getSchema(), false, plan.getToken());

        Schema prevOuterSchema = context.outerSchema;
        context.outerSchema = null;

        // Options cannot reference columns
        // CSOFF
        List<Option> options = plan.getOptions()
                .stream()
                .map(o -> new Option(o.getOption(), ColumnResolverVisitor.rewrite(context, o.getValueExpression())))
                .collect(toList());
        // CSON

        context.schema = prevSchema;
        context.outerSchema = prevOuterSchema;

        return new TableFunctionScan(plan.getTableSource(), plan.getSchema(), expressions, options, plan.getToken());
    }

    @Override
    public ILogicalPlan visit(TableScan plan, Ctx context)
    {
        context.pushAliasSchema(plan.getAlias(), plan.getSchema(), false, plan.getToken());
        return super.visit(plan, context);
    }

    @Override
    public ILogicalPlan visit(OverScan plan, Ctx context)
    {
        String alias = plan.getOverAlias();
        if (context.outerSchema == null)
        {
            throw new IllegalArgumentException("An over scan requires an outer schema");
        }

        int size = context.outerSchema.getSize();
        int matchOrdinal = -1;
        boolean containsAsterisks = false;
        for (int i = 0; i < size; i++)
        {
            Column column = context.outerSchema.getColumns()
                    .get(i);
            if (column.getType()
                    .getType() == Column.Type.TupleVector
                    && equalsIgnoreCase(alias, column.getName()))
            {
                if (matchOrdinal != -1)
                {
                    throw new ParseException("Ambiguous alias: " + alias, plan.getToken());
                }
                matchOrdinal = i;
            }

            containsAsterisks = containsAsterisks
                    || (column.getColumnReference() != null
                            && column.getColumnReference()
                                    .isAsterisk());
        }

        if (matchOrdinal == -1)
        {
            throw new ParseException("Alias " + alias + " cannot be bound", plan.getToken());
        }

        Column column = context.outerSchema.getColumns()
                .get(matchOrdinal);
        Schema schema = column.getType()
                .getSchema();

        // Cannot use ordinal when having asterisks
        if (containsAsterisks)
        {
            matchOrdinal = -1;
        }

        // We push the over alias to context schemas to override the outerschema that would otherwise
        // create an ambiguity due to the over scan will have a schema that is also present in the outer schema
        context.pushAliasSchema(alias, schema, false, plan.getToken());

        // An overscan is by definition outer reference so add the column to context
        context.outerReferences.add(column);

        return new OverScan(schema, alias, matchOrdinal, plan.getToken());
    }

    private List<IExpression> expandAsterisks(List<IExpression> expressions, Set<Column> outerReferences, Schema outerSchema, Schema inputSchema, Map<String, SchemaMapping> aliasSchema)
    {
        boolean inputSchemaHasAsterisks = inputSchema.getColumns()
                .stream()
                .anyMatch(c -> c.getColumnReference() != null
                        && c.getColumnReference()
                                .isAsterisk());

        List<IExpression> result = new ArrayList<>();
        for (IExpression expression : expressions)
        {
            if (expression instanceof AsteriskExpression)
            {
                AsteriskExpression ae = (AsteriskExpression) expression;
                QualifiedName qname = ae.getQname();

                int populateIndex = -1;
                String populateAlias = null;
                Schema schema = null;
                boolean outerReference = false;

                // Full schema
                if (qname.size() == 0)
                {
                    schema = inputSchema;
                }
                // Qualified schema
                else if (qname.size() == 1)
                {
                    SchemaMapping mapping = aliasSchema.get(qname.getFirst()
                            .toLowerCase());
                    if (mapping == null)
                    {
                        // Try outer schema
                        if (outerSchema != null)
                        {
                            for (Column outerCol : outerSchema.getColumns())
                            {
                                if (outerCol.getType()
                                        .getType() == Type.TupleVector
                                        && qname.getFirst()
                                                .equalsIgnoreCase(outerCol.getName()))
                                {
                                    outerReference = true;
                                    populateAlias = qname.getFirst()
                                            .toLowerCase();
                                    schema = outerCol.getType()
                                            .getSchema();
                                    populateIndex = outerSchema.getColumns()
                                            .indexOf(outerCol);

                                    inputSchemaHasAsterisks = outerSchema.getColumns()
                                            .stream()
                                            .anyMatch(c -> c.getColumnReference() != null
                                                    && c.getColumnReference()
                                                            .isAsterisk());

                                    outerReferences.add(outerCol);

                                    break;
                                }
                            }
                        }

                        if (schema == null)
                        {
                            throw new ParseException("Alias " + qname.toDotDelimited() + " could not be bound", ae.getToken());
                        }
                    }
                    else
                    {
                        schema = mapping.schema;
                        if (mapping.populated)
                        {
                            populateAlias = qname.getFirst()
                                    .toLowerCase();
                            // Expand the schema
                            populateIndex = inputSchema.getColumns()
                                    .indexOf(schema.getColumns()
                                            .get(0));
                            schema = schema.getColumns()
                                    .get(0)
                                    .getType()
                                    .getSchema();
                        }
                    }
                }
                // Expand nested value like map, tuple vector etc.
                else if (qname.size() > 1)
                {
                    throw new ParseException("Asterisk for nested values is not supported yet", ae.getToken());
                }

                int size = schema.getSize();
                for (int i = 0; i < size; i++)
                {
                    Column column = schema.getColumns()
                            .get(i);

                    // Internal columns should not be expanded
                    if (column.isInternal())
                    {
                        continue;
                    }

                    ColumnReference colRef = column.getColumnReference();

                    int index;
                    QualifiedName path;

                    if (populateAlias != null)
                    {
                        index = inputSchemaHasAsterisks ? -1
                                : populateIndex;

                        path = QualifiedName.of(populateAlias, column.getName());
                    }
                    else
                    {
                        index = inputSchemaHasAsterisks ? -1
                                : inputSchema.getColumns()
                                        .indexOf(column);

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
                        expressionType = ResolvedType.tupleVector(schema);
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
                        result.add(DereferenceExpression.create(builder.build(), path));
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
        public Void visit(SubQueryExpression expression, MutableBoolean context)
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
            List<IExpression> result = new ArrayList<>(expressions.size());
            for (IExpression expression : expressions)
            {
                result.add(expression.accept(INSTANCE, context));
            }
            return result;
        }

        @Override
        public IExpression visit(AggregateWrapperExpression expression, Ctx context)
        {
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
                    .accept(this, context), QualifiedName.of(expression.getRight()));
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

                    // Set schema mappings for all lambda identifiers
                    for (String lambdaIdentifier : le.getIdentifiers())
                    {
                        context.pushLambdaType(lambdaIdentifier, toArgType, null);
                    }

                    // Resolve the lambda's expression
                    arguments.set(lambdaArg, new LambdaExpression(le.getIdentifiers(), le.getExpression()
                            .accept(this, context), le.getLambdaIds()));

                    Map<String, SchemaMapping> schemaByAlias = context.schemaByAlias.peek();

                    // Remove all the lambda identifiers that we added before resolving
                    for (String lambdaIdentifier : le.getIdentifiers())
                    {
                        schemaByAlias.remove(lambdaIdentifier);
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
        public IExpression visit(SubQueryExpression expression, ColumnResolver.Ctx context)
        {
            // Push a new map for aliases. A sub query expression doesn't it self
            // have an alias but we are entering a new scope and we need a new clean map because of that
            context.schemaByAlias.push(new HashMap<>());

            // Reset the outer reference list before visiting the sub query expression plan
            // to distinctly gather outer references for this sub query
            Set<Column> prevOuterReferences = context.outerReferences;
            context.outerReferences = new HashSet<>();

            // Resolve the sub query plan
            ILogicalPlan input = expression.getInput()
                    .accept(context.columnResvoler, context);

            // CSOFF
            IExpression result = new SubQueryExpression(input, context.outerReferences, expression.getToken());
            // CSON

            // Pop the map we put all child schemas in since those are not needed anymore when we leave the subquery
            context.schemaByAlias.pop();

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
            Token token = expression.getToken();

            QualifiedName path = expression.getColumn();
            String alias;

            int lambdaId = expression.getLambdaId();
            if (lambdaId >= 0)
            {
                alias = path.getParts()
                        .get(0);
                path = path.extract(1);

                SchemaMapping schemaMapping = context.schemaByAlias.peek()
                        .get(alias);

                Column.Type type = schemaMapping.lambdaType.getType();
                // Non tuple vector type for the lambda then simply create a de-referenced column expression
                // else we need to resolve the expression against the schema
                if (type != Column.Type.TupleVector)
                {
                    return DereferenceExpression.create(ColumnExpression.Builder.of(alias, schemaMapping.lambdaType)
                            .withLambdaId(lambdaId)
                            .build(), path);
                }
            }
            else
            {
                alias = defaultString(path.getAlias(), "").toLowerCase();
                path = "".equals(alias) ? path
                        : path.extract(1);
            }

            String column = path.getFirst();
            path = path.extract(1);

            Schema schema;
            IExpression result = null;
            boolean populateAlias = false;
            boolean aliasMatch = false;
            // No alias, search in current and outer schema
            if ("".equals(alias))
            {
                schema = context.schema;
            }
            // Fetch schema from alias mapping
            else
            {
                Map<String, SchemaMapping> aliasMapping = context.schemaByAlias.peek();

                SchemaMapping mapping = aliasMapping != null ? aliasMapping.get(alias)
                        : null;

                aliasMatch = aliasMapping != null;

                // Extract the schema from the mapping
                schema = mapping != null ? mapping.schema
                        : null;
                populateAlias = mapping != null ? mapping.populated
                        : false;

                // Lambda expression with a schema ie. a TupleVector is accessed
                // resolve according to that schema
                if (mapping != null
                        && mapping.lambdaType != null)
                {
                    schema = mapping.lambdaType.getSchema();
                    // Special case where we don't access any columns but the whole lambda
                    // ie. map(x -> x) then we simply return the value as is
                    if ("".equals(column))
                    {
                        return ColumnExpression.Builder.of(alias, ResolvedType.tupleVector(schema))
                                .withLambdaId(lambdaId)
                                .build();
                    }

                    result = resolve(expression.getColumn(), schema, schema, false, null, column, null, lambdaId, token);

                    if (result == null)
                    {
                        throw new ParseException(expression.getColumn()
                                .toDotDelimited() + " cannot be bound", token);
                    }

                    return result;
                }
            }

            if (schema != null)
            {
                result = resolve(expression.getColumn(), context.schema, schema, false, populateAlias ? alias
                        : null, column, null, lambdaId, token);
            }

            // Search in outer scope
            // NOTE! Schema is the same as the outer schema when resolving a projection with subexpressions¨
            if (context.outerSchema != null
                    && schema != context.outerSchema)
            {
                IExpression maybeResult = result;

                boolean asteriskMatch = result != null
                        && result.getColumnReference() != null
                        && result.getColumnReference()
                                .getType() == ColumnReference.Type.NAMED_ASTERISK;

                Set<Column> outerReferences = context.outerReferences;
                context.outerReferences = new HashSet<>();

                // Result is still null or
                // we found an asterisk match and we have an outer schema then we must search in outer too
                // since there could be a non asterisk match there which have higher precedence
                // Only do search if we not explicitly found an alias match
                if (result == null
                        || (asteriskMatch
                                && !aliasMatch))
                {
                    result = resolve(expression.getColumn(), context.outerSchema, context.outerSchema, true, alias, column, context.outerReferences, -1, token);
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

            // No expression => cannot be bound
            if (result == null)
            {
                throw new ParseException(expression.getColumn()
                        .toDotDelimited() + " cannot be bound", token);
            }

            if (path.size() > 0)
            {
                return DereferenceExpression.create(result, path);
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

            boolean outerAsteriskMatch = outer != null
                    && outer.getColumnReference() != null
                    && outer.getColumnReference()
                            .getType() == ColumnReference.Type.NAMED_ASTERISK;

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
                ColumnReference colRef = ce.getColumnReference();
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

        private IExpression resolve(QualifiedName originalQname, Schema inputSchema, Schema schema, boolean compareAlias, String alias, String column, Set<Column> outerReferences, int lambdaId,
                Token token)
        {
            int asteriskIndexMatch = -1;
            int nonAsteriskIndexMatch = -1;
            boolean multipleAsterisk = false;
            boolean populatedColumnMatch = false;

            int size = schema.getSize();
            for (int i = 0; i < size; i++)
            {
                Column schemaColumn = schema.getColumns()
                        .get(i);
                ColumnReference colRef = schemaColumn.getColumnReference();
                boolean isAsterisk = colRef != null
                        && colRef.isAsterisk();
                String schemaAlias = colRef != null ? colRef.getTableSource()
                        .getAlias()
                        : "";

                if (compareAlias
                        && alias != null
                        && !"".equals(alias)
                        && !"".equals(schemaAlias)
                        && !alias.equalsIgnoreCase(schemaAlias))
                {
                    continue;
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
                                .getType() == Column.Type.TupleVector
                                && schemaColumn.getName()
                                        .equalsIgnoreCase(alias)))
                {
                    // Multiple matches of named columns is not allowed
                    if (nonAsteriskIndexMatch >= 0)
                    {
                        throw new ParseException("Ambiguous column: " + originalQname.toDotDelimited(), token);
                    }
                    nonAsteriskIndexMatch = i;

                    populatedColumnMatch = schemaColumn.getName()
                            .equalsIgnoreCase(alias);
                }
            }

            if (nonAsteriskIndexMatch == -1
                    && multipleAsterisk)
            {
                throw new ParseException("Ambiguous column: " + originalQname.toDotDelimited(), token);
            }
            else if (nonAsteriskIndexMatch == -1
                    && asteriskIndexMatch == -1)
            {
                return null;
            }

            int index = nonAsteriskIndexMatch >= 0 ? nonAsteriskIndexMatch
                    : asteriskIndexMatch;

            Column match = schema.getColumns()
                    .get(index);

            // Calculate ordinal of column in input schema
            // Ordinal can only be used if the whole schema consist of regular columns
            boolean canUseOrdinal = inputSchema.getColumns()
                    .stream()
                    .allMatch(c -> c.getColumnReference() == null
                            || c.getColumnReference()
                                    .getType() == ColumnReference.Type.REGULAR);

            int ordinal = canUseOrdinal ? inputSchema.getColumns()
                    .indexOf(match)
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

            ColumnReference colRef = match.getColumnReference();

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
                match = new Column(colRef.getName(), match.getType(), colRef);
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
                return DereferenceExpression.create(builder.build(), QualifiedName.of(column));
            }

            return builder.build();
        }
    }
}
